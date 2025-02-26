# SimpleParquetWriter

A lightweight, efficient .NET library for writing Parquet files with a simple builder pattern.

## Features

- Stream-like API for building dataframes row by row
- Background thread writing for improved performance
- Strong typing for various data types
- Minimal memory footprint
- No dictionary lookups for column names (uses ordered columns)
- Uses ParquetSharp under the hood

## Installation

```bash
dotnet add package Bendo.SimpleParquetWriter
```

## Quick Start

```csharp

// Create a writer for a new parquet file
using var writer = new SimpleParquetWriter("data.parquet");

// Add records one by one
writer.WriteField("name", "Alice");
writer.WriteField("age", 28);
writer.WriteField("active", true);
writer.WriteField("score", 94.5);
writer.WriteField("hired_date", DateTime.UtcNow);
writer.NextRecord();

writer.WriteField("name", "Bob");
writer.WriteField("age", 32);
writer.WriteField("active", false);
writer.WriteField("score", 88.0);
writer.WriteField("hired_date", DateTime.UtcNow.AddYears(-2));
writer.NextRecord();

// More records...

// The file is written on a background thread and closed when the writer is disposed (dispose blocks the current thread).
```

## Important Notes

- The parquet file is written in a background thread and finalized when the writer is disposed
- Column order must be the same for every row (this improves performance by avoiding dictionary lookups)
- The writer supports various .NET types 

## Configuration

### Logging

By default, the library logs to the console, but you can configure your own logging:

```csharp
// Set custom logger
SimpleParquetWriter.Logger = (message) => _myLoggingFramework.Log(message);
```

### Column Order Validation

For development or debugging, you can enable column order validation:

```csharp
// Enable column order validation (default is false for performance)
SimpleParquetWriter.VerifyColumnOrder = true;
```

## Performance Considerations

- Column names are not used for dictionary lookups, making field writing very efficient
- Data is buffered in memory and written in a background thread
- Make sure to properly dispose the writer to ensure all data is written correctly

## License

MIT