using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Text.Unicode;
using System.Threading.Tasks;

namespace AddressParserAndUpdater
{
    public class AddressUpdateService
    {
        private readonly string connectionString;
        private const int BatchSize = 100;
        public Dictionary<object, string> parseFailureRemarks = new Dictionary<object, string>();
        public Dictionary<object, string> noChangesParseRemarks = new Dictionary<object, string>();
        public Dictionary<object, string> successfulParseRemarks = new Dictionary<object, string>();
        public Dictionary<string, Dictionary<object, string>> ParseRemarks = new Dictionary<string, Dictionary<object, string>>();
        private readonly string exportDirectoryPath = "ExportedFiles"; // שם התיקיה שתכיל את הקבצים המיוצאים
        private readonly string baseFilePath = "ParseSummary.json"; // השם הבסיסי של הקובץ

        public AddressUpdateService()
        {
            connectionString = "Server=stldr095;Database=Kada-22;User Id=KADAUser;Password=123456;";

            ParseRemarks.Add("ParseFailureRemarks", parseFailureRemarks);
            ParseRemarks.Add("NoChangesParseRemarks", noChangesParseRemarks);
            ParseRemarks.Add("SuccessfulParseRemarks", successfulParseRemarks);
        }

        public async Task<DataTable> GetAddressesAsync(int startIndex, int batchSize)
        {
            BlueConsoleWriteLine("Connecting to the database to get addresses...");

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                string query = $@"WITH AddressCTE AS (
                                      SELECT 
                                          [AddressID], [StudentID], [InstitutionID], [SchoolID], [IsMain], [IsMail], [CityID], 
                                          [StreetID], [HouseNum], [ZipCode], [Neighborhood], [RegionID], [Remark],
                                          ROW_NUMBER() OVER (ORDER BY [AddressID]) AS RowNumber
                                      FROM [tblAddress] (NOLOCK)
                                      WHERE Remark IS NOT NULL
                                  )
                                  SELECT 
                                      [AddressID], [StudentID], [InstitutionID], [SchoolID], [IsMain], [IsMail], [CityID], 
                                      [StreetID], [HouseNum], [ZipCode], [Neighborhood], [RegionID], [Remark]
                                  FROM AddressCTE
                                  WHERE RowNumber BETWEEN @StartIndex AND @EndIndex";

                using (SqlDataAdapter adapter = new SqlDataAdapter(query, connection))
                {
                    adapter.SelectCommand.Parameters.AddWithValue("@StartIndex", startIndex + 1);
                    adapter.SelectCommand.Parameters.AddWithValue("@EndIndex", startIndex + batchSize);

                    DataTable addresses = new DataTable();
                    await Task.Run(() => adapter.Fill(addresses));
                    return addresses;
                }

            }
        }

        public async Task<int> GetRemarkCountAsync()
        {
            string remarkToChangeCountQuery = @"SELECT COUNT(*)
                                                FROM [Kada-22].[dbo].[tblAddress] (NOLOCK)
                                                WHERE Remark IS NOT NULL";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (SqlCommand command = new SqlCommand(remarkToChangeCountQuery, connection))
                {
                    int count = (int)await command.ExecuteScalarAsync();
                    YellowConsoleWriteLine($"Found {count} addresses for update.");
                    return count;
                }
            }
        }

        DateTime startTime;
        DateTime endTime;
        TimeSpan duration;
        public async Task ProcessAddressesInBatchesAsync()
        {
            // הקלטת זמן התחלה
            startTime = DateTime.Now;
            Console.WriteLine($"Start Time: {startTime:HH:mm:ss}");

            Console.WriteLine("Processing addresses in batches...");

            int startIndex = 0;
            DataTable addresses;

            GetRemarkCountAsync();

            do
            {
                addresses = await GetAddressesAsync(startIndex, 20);
                //addresses = await GetAddressesAsync(startIndex, BatchSize);


                // עיבוד כתובות במקביל
                var processTasks = addresses.AsEnumerable().Select(row => ProcessAddressAsync(row["Remark"].ToString(), row)).ToArray();
                await Task.WhenAll(processTasks);

                // foreach (DataRow row in addresses.Rows)
                // {
                //     string rawAddress = row["Remark"].ToString();
                //     await ProcessAddressAsync(rawAddress, row);
                // }

                await UpdateAddressesAsync(addresses);

                startIndex += BatchSize;

            } while (false); // ממשיך לעבד עד שאין עוד כתובות לעבד
            //} while (addresses.Rows.Count > 0); // ממשיך לעבד עד שאין עוד כתובות לעבד

            // הקלטת זמן סיום
            endTime = DateTime.Now;
            Console.WriteLine($"End Time: {endTime:HH:mm:ss}");

            // חישוב הזמן שחלף
            duration = endTime - startTime;
            // הצגת הזמן שחלף בפורמט של שעות, דקות ושניות
            Console.WriteLine($"Duration: {duration.Hours} hours, {duration.Minutes} minutes, {duration.Seconds} seconds");

            PrintSummary();
        }

        public async Task ProcessAddressAsync(string rawAddress, DataRow addressRow)
        {
            if (string.IsNullOrEmpty(rawAddress)) return;

            var (streetName, houseNumber, city, remark) = ParseAddress(rawAddress);
            Console.WriteLine($"Parsed Address: {streetName} {houseNumber} {city} {remark}");

            if (string.IsNullOrEmpty(remark))
            {
                var (streetID, cityID) = await GetStreetAndCityIDAsync(addressRow["CityID"] as int?, city, streetName);

                if (streetID.HasValue)
                {
                    //Console.WriteLine($"Found StreetID: {streetID}, CityID: {cityID}");

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

            RedConsoleWriteLine($"Address parsing failed for: {address}");
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
                                       FROM [sysStreet] (NOLOCK)
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
                                         FROM [sysCity] (NOLOCK)
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
                                            FROM [sysStreet] (NOLOCK)
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
            BlueConsoleWriteLine("Updating addresses...");
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
                            string selectQuery = @"SELECT [StreetID], [HouseNum], [CityID] 
                                           FROM [tblAddress] (NOLOCK)
                                           WHERE [AddressID] = @AddressID";
                            bool hasChanges = false;

                            using (SqlCommand selectCommand = new SqlCommand(selectQuery, connection, transaction))
                            {
                                selectCommand.Parameters.AddWithValue("@AddressID", row["AddressID"]);
                                using (SqlDataReader reader = await selectCommand.ExecuteReaderAsync())
                                {
                                    if (await reader.ReadAsync())
                                    {
                                        // Check if the fields are different before and after the update
                                        if (!Equals(reader["StreetID"], row["StreetID"]) ||
                                            !Equals(reader["HouseNum"], row["HouseNum"]) ||
                                            !Equals(reader["CityID"], row["CityID"]))
                                        {
                                            hasChanges = true;
                                        }
                                        else if (row["StreetID"] == DBNull.Value)
                                        {
                                            parseFailureRemarks[Convert.ToInt32(row["AddressID"])] = row["Remark"].ToString();
                                            RedConsoleWriteLine($"No changes detected for AddressID {row["AddressID"]}, conversion and parsing remark failed! ({row["Remark"]})");
                                        }
                                        else
                                        {
                                            noChangesParseRemarks[Convert.ToInt32(row["AddressID"])] = row["Remark"].ToString();
                                            RedConsoleWriteLine($"No changes detected for AddressID {row["AddressID"]}, there is no change detected!");
                                        }
                                    }
                                    else
                                    {
                                        GreenConsoleWriteLine($"No data found for AddressID {row["AddressID"]}");
                                    }
                                    ResetConsoleColor();
                                }
                            }

                            if (hasChanges)
                            {
                                string updateQuery = @"UPDATE [tblAddress]
                                               SET [StreetID] = @StreetID, [HouseNum] = @HouseNum, [CityID] = @CityID
                                               WHERE [AddressID] = @AddressID";

                                using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection, transaction))
                                {
                                    updateCommand.Parameters.AddWithValue("@StreetID", row["StreetID"]);
                                    updateCommand.Parameters.AddWithValue("@HouseNum", row["HouseNum"]);
                                    updateCommand.Parameters.AddWithValue("@CityID", row["CityID"]);
                                    updateCommand.Parameters.AddWithValue("@AddressID", row["AddressID"]);

                                    await updateCommand.ExecuteNonQueryAsync();
                                    successfulParseRemarks[Convert.ToInt32(row["AddressID"])] = row["Remark"].ToString();
                                    GreenConsoleWriteLine($"Successfully updated AddressID {row["AddressID"]}");
                                }
                            }
                        }

                        transaction.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                RedConsoleWriteLine($"Error occurred: {ex.Message}");
            }
        }


        public void PrintSummary()
        {
            BlueConsoleWriteLine("Processing Summary:");
            GreenConsoleWriteLine($"ParseFailureCount: {ParseRemarks["ParseFailureRemarks"].Count}");
            GreenConsoleWriteLine($"NoChangesCount: {ParseRemarks["NoChangesParseRemarks"].Count}");
            GreenConsoleWriteLine($"SuccessfulParseCount: {ParseRemarks["SuccessfulParseRemarks"].Count}");
            GreenConsoleWriteLine($"TotalCount: {ParseRemarks["ParseFailureRemarks"].Count + ParseRemarks["NoChangesParseRemarks"].Count + ParseRemarks["SuccessfulParseRemarks"].Count}");

            ExportParseSummary();
        }

        public void ExportParseSummary()
        {
            try
            {
                // יצירת תיקיה לייצוא אם לא קיימת
                string directory = GetExportDirectory();
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // יצירת נתיב קובץ עם תאריך ושעה נוכחיים
                string filePath = GetFilePathWithDate(directory, baseFilePath);

                // הוספת סיכום למילון ParseRemarks
                var summary = new Dictionary<object, int>
                {
                    { "ParseFailureCount", ParseRemarks.ContainsKey("ParseFailureRemarks") ? ParseRemarks["ParseFailureRemarks"].Count : 0 },
                    { "NoChangesCount", ParseRemarks.ContainsKey("NoChangesParseRemarks") ? ParseRemarks["NoChangesParseRemarks"].Count : 0 },
                    { "SuccessfulParseCount", ParseRemarks.ContainsKey("SuccessfulParseRemarks") ? ParseRemarks["SuccessfulParseRemarks"].Count : 0 },
                    { "TotalCount", (ParseRemarks.ContainsKey("ParseFailureRemarks") ? ParseRemarks["ParseFailureRemarks"].Count : 0) +
                                      (ParseRemarks.ContainsKey("NoChangesParseRemarks") ? ParseRemarks["NoChangesParseRemarks"].Count : 0) +
                                      (ParseRemarks.ContainsKey("SuccessfulParseRemarks") ? ParseRemarks["SuccessfulParseRemarks"].Count : 0) },
                    { "DurationMinutes", (int)duration.TotalMinutes }
                };

                ParseRemarks.Add("Summary", summary.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToString()));

                PrintDictionary(ParseRemarks);

                // המרת ה- Dictionary ל- JSON וכתיבתו לקובץ
                var options = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
                };

                string json = JsonSerializer.Serialize(ParseRemarks, options);
                File.WriteAllText(filePath, json);
                Console.WriteLine(json); // הדפס לקונסול לבדוק את התוכן

                YellowConsoleWriteLine($"Summary of Parsing Failures exported to: {filePath}");
            }
            catch (Exception ex)
            {
                RedConsoleWriteLine("Export failed!");
                RedConsoleWriteLine($"Error: {ex.Message}");
            }
        }

        static void PrintDictionary(Dictionary<string, Dictionary<object, string>> dictionary)
        {
            foreach (var outerPair in dictionary)
            {
                Console.WriteLine($"{outerPair.Key} ({outerPair.Value.Count})");
                foreach (var innerPair in outerPair.Value)
                {
                    Console.WriteLine($"\tAddresID: {innerPair.Key}, Value: {innerPair.Value}");
                }
                Console.WriteLine();
            }
        }

        // פונקציה ליצירת נתיב קובץ עם תאריך ושעה
        private string GetFilePathWithDate(string directory, string baseFilePath)
        {
            // קבלת התאריך והשעה הנוכחיים בפורמט YYYY-MM-DD_HH-mm-ss
            string dateTimeStamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss");

            // יצירת נתיב קובץ חדש עם התאריך והשעה
            string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(baseFilePath);
            string extension = Path.GetExtension(baseFilePath);

            string filePath = Path.Combine(directory, $"{fileNameWithoutExtension}_{dateTimeStamp}{extension}");

            return filePath;
        }

        // פונקציה לקבלת הנתיב של התיקיה שבה נמצא הקובץ של הסרוויס
        private string GetExportDirectory()
        {
            // קבלת נתיב התיקיה של קובץ הסרוויס (קובץ הסרוויס עצמו)
            string assemblyLocation = typeof(AddressUpdateService).Assembly.Location;

            // קבלת נתיב התיקיה של קובץ הסרוויס, חזרה אחורה לתיקיית הפרויקט
            string projectDirectory = Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(assemblyLocation))));

            // יצירת נתיב לתיקיה שבה יישמר הקובץ
            string exportDirectory = Path.Combine(projectDirectory, exportDirectoryPath);

            return exportDirectory;
        }

        public void GreenConsoleWriteLine(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(msg);
            ResetConsoleColor();
        }
        public void RedConsoleWriteLine(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(msg);
            ResetConsoleColor();
        }
        public void BlueConsoleWriteLine(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine(msg);
            ResetConsoleColor();
        }
        public void YellowConsoleWriteLine(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(msg);
            ResetConsoleColor();
        }
        public void ResetConsoleColor()
        {
            Console.ResetColor();
        }

    }
}
