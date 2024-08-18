using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AddressParserAndUpdater
{
    public class AddressUpdateService
    {
        private readonly string connectionString;

        public AddressUpdateService()
        {
            // הגדרת מחרוזת החיבור ישירות בקוד
            connectionString = "Server=stldr095;Database=KADA_Dev_MOD;User Id=KADAUser;Password=123456;";
        }

        public async Task<DataTable> GetAddressesAsync()
        {
            BlueConsoleColor();
            Console.WriteLine("Connecting to the database to get addresses...");

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                string query = @"SELECT TOP 5 [AddressID], [StudentID], [InstitutionID], [SchoolID], [IsMain], [IsMail], [CityID], [StreetID], [HouseNum], [ZipCode], [Neighborhood], [RegionID], [Remark]
                                FROM [tblAddress]
                                WHERE Remark IS NOT NULL AND AddressID IN (2436640, 2436645, 2436646, 2436647, 2436648)";
                

                using (SqlDataAdapter adapter = new SqlDataAdapter(query, connection))
                {
                    DataTable addresses = new DataTable();
                    await Task.Run(() => adapter.Fill(addresses));
                    Console.WriteLine("Addresses retrieved successfully.");
                    ResetConsoleColor();
                    return addresses;
                }
            }

        }

        public async Task<DataTable> ProcessAddressesAsync()
        {
            Console.WriteLine("Processing addresses...");

            DataTable addresses = await GetAddressesAsync();

            foreach (DataRow row in addresses.Rows)
            {
                string rawAddress = row["Remark"].ToString();
                Console.WriteLine($"Processing address: {rawAddress}");
                await ProcessAddressAsync(rawAddress, row);
            }
            return addresses;
        }

        public async Task ProcessAddressAsync(string rawAddress, DataRow addressRow)
        {
            if (string.IsNullOrEmpty(rawAddress)) return;

            var (streetName, houseNumber, city, remark) = ParseAddress(rawAddress);
            Console.WriteLine($"Parsed Address - Street: {streetName}, House Number: {houseNumber}, City: {city}, Remark: {remark}");

            if (string.IsNullOrEmpty(remark))
            {
                var (streetID, cityID) = await GetStreetAndCityIDAsync(addressRow["CityID"] as int?, city, streetName);
                Console.WriteLine($"Found StreetID: {streetID}, CityID: {cityID}");

                if (streetID.HasValue)
                {
                    addressRow["StreetID"] = streetID;
                    addressRow["HouseNum"] = houseNumber;
                    if (addressRow["CityID"] == DBNull.Value && cityID.HasValue)
                    {
                        addressRow["CityID"] = cityID;
                    }
                }
            }
        }

        private (string StreetName, string HouseNumber, string City, string Remark) ParseAddress(string address)
        {
            string streetName = null, houseNumber = null, city = null, remark = null;

            var patterns = new[]
            {
                // דפוס: תיבת דואר (ת.ד)
                @"^ת\.ד\s*(?<Remark>\d+)$",

                // דפוס: רחוב, מספר בית עם מספר דירה
                @"^(?<StreetName>[^\d,]+?)\s+(?<HouseNumber>\d+)(?:\s*דירה\s*(?<ApartmentNumber>\d+))?\s*(?:[,;]\s*(?<City>[^\d,]*))?$",

                // דפוס: רחוב, מספר בית עם אותיות
                @"^(?<StreetName>[^\d,]+?)\s+(?<HouseNumber>\d+[א-ת]?(?:/\d*[א-ת]?)?(?:,\s*\d+[א-ת]?)?)\s*(?:[,;]\s*(?<City>[^\d,]*))?$",

                // דפוס: רחוב, מספר בית, עיר (עובד גם ללא פסיק אחרי המספר)
                @"^(?<StreetName>[^\d,]+?)\s+(?<HouseNumber>\d+[א-ת']*)\s+(?<City>[^\d,]+)$",

                // דפוס: מספר בית, רחוב, עיר
                @"^(?<HouseNumber>\d+[א-ת'/-]*)\s+(?<StreetName>[^\d,]+?)\s*(?:[,;]\s*(?<City>[^\d,]*))?$",

                // דפוס: רחוב בלבד
                @"^(?<StreetName>[^\d,]+?)$"
            };

            foreach (var pattern in patterns)
            {
                var match = Regex.Match(address, pattern, RegexOptions.IgnoreCase);
                if (match.Success)
                {
                    streetName = match.Groups["StreetName"].Value.Replace("רחוב", "").Trim();
                    houseNumber = match.Groups["HouseNumber"].Value.Trim();
                    city = match.Groups["City"].Value.Trim();
                    remark = match.Groups["Remark"].Value.Trim();

                    if (match.Groups["ApartmentNumber"].Success)
                    {
                        houseNumber = $"{houseNumber}/{match.Groups["ApartmentNumber"].Value.Trim()}";
                    }

                    //מס' בית + כניסה
                    if (!string.IsNullOrEmpty(city) && Regex.IsMatch(city, @"^[א-ת]('|)?$", RegexOptions.IgnoreCase))
                    {
                        houseNumber = $"{houseNumber} {city}".Trim();
                        city = null;
                    }

                    return (streetName, houseNumber, city, remark);
                }
            }

            RedConsoleColor();
            Console.WriteLine($"Address parsing failed for: {address}");
            return (null, null, null, address); // אם אין התאמה, החזר את הכתובת כהערה
        }

        private async Task<(int? StreetID, int? CityID)> GetStreetAndCityIDAsync(int? cityID, string cityName, string streetName)
        {
            int? foundStreetID = null;
            int? foundCityID = cityID;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // חיפוש רחוב לפי CityID ו-StreetName
                string streetQuery = @"SELECT [StreetID]
                                        FROM [sysStreet]
                                        WHERE [CityID] = @CityID AND [StreetName] = @StreetName";

                using (SqlCommand streetCommand = new SqlCommand(streetQuery, connection))
                {
                    streetCommand.Parameters.AddWithValue("@CityID", cityID.HasValue ? (object)cityID.Value : DBNull.Value);
                    streetCommand.Parameters.AddWithValue("@StreetName", streetName);

                    var result = await streetCommand.ExecuteScalarAsync();
                    if (result != null)
                    {
                        foundStreetID = Convert.ToInt32(result);
                    }
                }

                // אם לא נמצא רחוב עם ה-CityID הקיים, חפש עיר לפי cityName
                if (!foundStreetID.HasValue && !string.IsNullOrEmpty(cityName))
                {
                    string cityQuery = @"SELECT [CityID]
                                         FROM [sysCity]
                                         WHERE [CityName] = @CityName";

                    using (SqlCommand cityCommand = new SqlCommand(cityQuery, connection))
                    {
                        cityCommand.Parameters.AddWithValue("@CityName", cityName);

                        var cityResult = await cityCommand.ExecuteScalarAsync();
                        if (cityResult != null)
                        {
                            foundCityID = Convert.ToInt32(cityResult);

                            // לאחר שמצאנו CityID חדש, חפש את ה-StreetID מחדש עם ה-CityID החדש
                            streetQuery = @"SELECT [StreetID]
                                            FROM [sysStreet]
                                            WHERE [CityID] = @CityID AND [StreetName] = @StreetName";

                            using (SqlCommand streetCommand2 = new SqlCommand(streetQuery, connection))
                            {
                                streetCommand2.Parameters.AddWithValue("@CityID", foundCityID.HasValue ? (object)foundCityID.Value : DBNull.Value);
                                streetCommand2.Parameters.AddWithValue("@StreetName", streetName);

                                var streetResult = await streetCommand2.ExecuteScalarAsync();
                                if (streetResult != null)
                                {
                                    foundStreetID = Convert.ToInt32(streetResult);
                                }
                            }
                        }
                    }
                }
            }

            return (foundStreetID, foundCityID);
        }

        public async Task UpdateAddressesAsync(DataTable addresses)
        {
            Console.WriteLine("Updating addresses...");
            ResetConsoleColor();
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    using (SqlTransaction transaction = connection.BeginTransaction())
                    {
                        foreach (DataRow row in addresses.Rows)
                        {
                            string selectQuery = "SELECT [StreetID], [HouseNum], [CityID] FROM [tblAddress] WHERE [AddressID] = @AddressID";
                            bool hasChanges = false;

                            using (SqlCommand selectCommand = new SqlCommand(selectQuery, connection, transaction))
                            {
                                selectCommand.Parameters.AddWithValue("@AddressID", row["AddressID"]);
                                using (SqlDataReader reader = await selectCommand.ExecuteReaderAsync())
                                {
                                    if (await reader.ReadAsync())
                                    {
                                        if (!Equals(reader["StreetID"], row["StreetID"]) ||
                                            !Equals(reader["HouseNum"], row["HouseNum"]) ||
                                            !Equals(reader["CityID"], row["CityID"]))
                                        {
                                            hasChanges = true;
                                        }
                                        else
                                        {
                                            RedConsoleColor();
                                            Console.WriteLine($"No changes detected for AddressID {row["AddressID"]}");
                                        }
                                    }
                                    else
                                    {
                                        GreenConsoleColor();
                                        Console.WriteLine($"No data found for AddressID {row["AddressID"]}");
                                    }
                                    ResetConsoleColor();
                                }
                            }

                            if (hasChanges)
                            {
                                string query = "UPDATE [tblAddress] SET [StreetID] = @StreetID, [HouseNum] = @HouseNum, [CityID] = @CityID WHERE [AddressID] = @AddressID";
                                using (SqlCommand command = new SqlCommand(query, connection, transaction))
                                {
                                    command.Parameters.AddWithValue("@StreetID", row["StreetID"] ?? (object)DBNull.Value);
                                    command.Parameters.AddWithValue("@HouseNum", row["HouseNum"] ?? (object)DBNull.Value);
                                    command.Parameters.AddWithValue("@CityID", row["CityID"] ?? (object)DBNull.Value);
                                    command.Parameters.AddWithValue("@AddressID", row["AddressID"]);

                                    int rowsAffected = await command.ExecuteNonQueryAsync();
                                    GreenConsoleColor();
                                    Console.WriteLine($"Updated AddressID {row["AddressID"]}: {rowsAffected} rows affected.");
                                    ResetConsoleColor();
                                }
                            }
                        }
                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                // טיפול בשגיאות
                RedConsoleColor();
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }


        public void GreenConsoleColor()
        {
            Console.ForegroundColor = ConsoleColor.Green;
        }
        public void RedConsoleColor()
        {
            Console.ForegroundColor = ConsoleColor.Red;
        }
        public void BlueConsoleColor()
        {
            Console.ForegroundColor = ConsoleColor.Blue;
        }
        public void ResetConsoleColor()
        {
            Console.ResetColor();
        }
    }
}

