using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using System.Threading.Tasks;

namespace AddressParserAndUpdater.help
{
    public class ConvertJson
    {
        public ConvertJson()
        {
            ConvertJsonToReadableFormat(@"C:\Users\batshevae\Desktop\Kada\scripts\AddressParserAndUpdater\AddressParserAndUpdater\ExportedFiles\ParseSummary_2024-08-20_13-21-41.json", @"C:\Users\batshevae\Desktop\Kada\scripts\AddressParserAndUpdater\AddressParserAndUpdater\ExportedFiles\ParseSummary_2024-08-20_13-21-41 - Copy.json");
        }
        void ConvertJsonToReadableFormat(string oldFilePath, string newFilePath)
        {
            try
            {
                // קריאת התוכן מהקובץ הישן
                string oldJsonContent = File.ReadAllText(oldFilePath);

                // המרת ה-JSON לאובייקט דינמי
                var jsonObject = JsonSerializer.Deserialize<object>(oldJsonContent);

                // הגדרת אפשרויות לסידור מחדש של האובייקט לפורמט קריא
                var options = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
                };

                // המרת האובייקט ל-JSON קריא ושמירה בקובץ החדש
                string newJsonContent = JsonSerializer.Serialize(jsonObject, options);
                File.WriteAllText(newFilePath, newJsonContent);

                Console.WriteLine($"File successfully converted and saved to: {newFilePath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Conversion failed!");
                Console.WriteLine($"Error: {ex.Message}");
            }
        }
    }
}
