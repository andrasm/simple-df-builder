using System.Diagnostics;

namespace Bendo.SimpleDataFrameBuilder.Tests;

[Explicit]
public class Benchmark
{
    private const int Rows = 2_000_000;
    private const int Iterations = 5;

    [Test]
    public void Bench()
    {
        Run(50_000);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var gc0 = GC.CollectionCount(0);
        var gc1 = GC.CollectionCount(1);
        var gc2 = GC.CollectionCount(2);
        var allocBefore = GC.GetTotalAllocatedBytes(true);

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < Iterations; i++)
        {
            Run(Rows);
        }
        sw.Stop();

        var allocAfter = GC.GetTotalAllocatedBytes(true);
        var totalRows = (long)Rows * Iterations;
        var mb = (allocAfter - allocBefore) / 1024.0 / 1024.0;

        Console.WriteLine("");
        Console.WriteLine($"=== Benchmark: {Iterations} iters x {Rows:N0} rows = {totalRows:N0} rows ===");
        Console.WriteLine($"Time:        {sw.Elapsed.TotalSeconds:F2} s");
        Console.WriteLine($"Throughput:  {totalRows / sw.Elapsed.TotalSeconds:N0} rows/sec");
        Console.WriteLine($"Allocated:   {mb:F1} MB ({mb * 1024 * 1024 / totalRows:F1} bytes/row)");
        Console.WriteLine($"GC gen0:     {GC.CollectionCount(0) - gc0}");
        Console.WriteLine($"GC gen1:     {GC.CollectionCount(1) - gc1}");
        Console.WriteLine($"GC gen2:     {GC.CollectionCount(2) - gc2}");
    }

    private static void Run(int rows)
    {
        var path = Path.Combine(Path.GetTempPath(), $"bench_{Guid.NewGuid():N}.parquet");
        try
        {
            using var w = new SimpleParquetWriter(path, recordsInRowGroup: 65536);
            var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (int i = 0; i < rows; i++)
            {
                w.WriteField("ts", dt.AddSeconds(i));
                w.WriteField("price", 100.5 + i * 0.01);
                w.WriteField("size", (long)i);
                w.WriteField("symbol", "AAPL");
                w.WriteField("flag", i % 2 == 0);
                w.WriteField("vol", 1.5f + i);
                w.WriteField("seq", i);
                w.WriteField("notional", (double)(i * 100));
                w.NextRecord();
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }
}
