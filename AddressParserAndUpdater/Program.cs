// See https://aka.ms/new-console-template for more information
using AddressParserAndUpdater;
using System.Data;

Console.WriteLine("Hello, World!");

AddressUpdateService service = new AddressUpdateService();
Console.WriteLine("Retrieving addresses...");

// אסינכרוני - השתמש ב-async/await כדי להמתין להשלמת הפעולה
DataTable addresses = await service.GetAddressesAsync();

Console.WriteLine($"Found {addresses.Rows.Count} addresses.");

// אסינכרוני - השתמש ב-async/await כדי להמתין להשלמת הפעולה

addresses = await service.ProcessAddressesAsync();

service.BlueConsoleColor();
Console.WriteLine("Processing completed.");

// אסינכרוני - השתמש ב-async/await כדי להמתין להשלמת הפעולה
await service.UpdateAddressesAsync(addresses);

Console.WriteLine("Addresses updated.");

Console.ReadKey();
