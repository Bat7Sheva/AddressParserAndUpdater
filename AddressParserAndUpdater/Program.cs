// See https://aka.ms/new-console-template for more information
using AddressParserAndUpdater;
using System.Data;

AddressUpdateService service = new AddressUpdateService();
Console.WriteLine("Retrieving addresses...");

// אסינכרוני - השתמש ב-async/await כדי להמתין להשלמת הפעולה
DataTable addresses = await service.ProcessAddressesAsync();

service.BlueConsoleColor();
Console.WriteLine("Processing completed.");

// אסינכרוני - השתמש ב-async/await כדי להמתין להשלמת הפעולה
await service.UpdateAddressesAsync(addresses);
Console.WriteLine("Addresses updated.");

service.PrintSummery();

Console.ReadKey();
