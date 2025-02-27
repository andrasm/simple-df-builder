# SimpleParquetWriter

A lightweight, efficient .NET library for writing Parquet files with a simple builder pattern.

## Features

- Stream-like API for building dataframes row by row
- Background thread writing for improved performance
- Reasonable defaults for most use cases:
  - zstandard compression
  - disabled dictionary encoding for floating point numbers
  - 64K rowgroups
- Strong typing for various data types
- Minimal memory footprint and allocations
- No dictionary lookups for column names (uses ordered columns)
- Uses ParquetSharp under the hood

## Installation

```bash
dotnet add package Bendo.SimpleDataFrameBuilder
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

// The file is written on a background thread and closed when the writer 
// is disposed (dispose blocks the current thread).
```

## Important Notes

- Column order must be the same for every row (this improves performance by avoiding dictionary lookups)
- The parquet file is written in a background thread and finalized when the writer is disposed
- The writer must be disposed to ensure all data is written correctly
- Try to avoid string construction in the `WriteField` method to reduce allocations (use string constants or reuse strings). 
  - You can use the optional `int idx` argument to help with this, e.g.:

#### Example: use the optional `idx` argument to avoid string construction:
```csharp
for (int i = 0; i < 3; i++)
{    
    writer.WriteField("address_line_{0}", addresses[i], i);
}

// ...

// is equivalent to:
writer.WriteField("address_line_0", addresses[0]);
writer.WriteField("address_line_1", addresses[1]);
writer.WriteField("address_line_2", addresses[2]);
```

## Configuration

### Logging

By default, the library logs a few events to the console, but you can configure your own logging:

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

## Project status

Stable and used in production by the author.

## License

MIT