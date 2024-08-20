// See https://aka.ms/new-console-template for more information
using AddressParserAndUpdater;

AddressUpdateService service = new AddressUpdateService();
Console.WriteLine("Retrieving addresses...");

await service.ProcessAddressesInBatchesAsync();

service.BlueConsoleWriteLine("Processing completed.");

Console.ReadKey();
