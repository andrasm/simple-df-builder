using ParquetSharp;

namespace Bendo.SimpleDataFrameBuilder.Tests;

public class RoundTripTests
{
    private static string TempFile() =>
        Path.Combine(Path.GetTempPath(), $"sdf_test_{Guid.NewGuid():N}.parquet");

    private static T[] ReadCol<T>(RowGroupReader rg, int col, int n)
    {
        using var lr = rg.Column(col).LogicalReader<T>();
        return lr.ReadAll(n);
    }

    [Test]
    public void RoundTrip_AllSupportedTypes()
    {
        var path = TempFile();
        try
        {
            var dt = new DateTime(2024, 1, 15, 10, 30, 45, DateTimeKind.Utc);
            using (var w = new SimpleParquetWriter(path, recordsInRowGroup: 8))
            {
                for (int i = 0; i < 5; i++)
                {
                    w.WriteField("i32", 100 + i);
                    w.WriteField("i64", 1_000_000_000L + i);
                    w.WriteField("i16", (short)(10 + i));
                    w.WriteField("u64", 2_000_000_000UL + (uint)i);
                    w.WriteField("f32", 1.5f + i);
                    w.WriteField("f64", 2.5 + i);
                    w.WriteField("flag", i % 2 == 0);
                    w.WriteField("dt", dt.AddSeconds(i));
                    w.WriteField("dec", (decimal)(3.25 + i));
                    w.WriteField("str", $"row_{i}");
                    w.NextRecord();
                }
            }

            using var r = new ParquetFileReader(path);
            Assert.That(r.FileMetaData.NumRowGroups, Is.EqualTo(1));
            using var rg = r.RowGroup(0);
            Assert.That(rg.MetaData.NumRows, Is.EqualTo(5));

            var i32 = ReadCol<int>(rg, 0, 5);
            var i64 = ReadCol<long>(rg, 1, 5);
            var i16 = ReadCol<short>(rg, 2, 5);
            var u64 = ReadCol<ulong>(rg, 3, 5);
            var f32 = ReadCol<float>(rg, 4, 5);
            var f64 = ReadCol<double>(rg, 5, 5);
            var flag = ReadCol<bool>(rg, 6, 5);
            var dts = ReadCol<DateTime>(rg, 7, 5);
            var dec = ReadCol<double>(rg, 8, 5);
            var str = ReadCol<string>(rg, 9, 5);

            for (int i = 0; i < 5; i++)
            {
                Assert.That(i32[i], Is.EqualTo(100 + i));
                Assert.That(i64[i], Is.EqualTo(1_000_000_000L + i));
                Assert.That(i16[i], Is.EqualTo((short)(10 + i)));
                Assert.That(u64[i], Is.EqualTo(2_000_000_000UL + (uint)i));
                Assert.That(f32[i], Is.EqualTo(1.5f + i));
                Assert.That(f64[i], Is.EqualTo(2.5 + i));
                Assert.That(flag[i], Is.EqualTo(i % 2 == 0));
                Assert.That(dts[i], Is.EqualTo(dt.AddSeconds(i)));
                Assert.That(dec[i], Is.EqualTo(3.25 + i));
                Assert.That(str[i], Is.EqualTo($"row_{i}"));
            }
        }
        finally { TryDelete(path); }
    }

    [Test]
    public void RoundTrip_NonAsciiInlineStrings()
    {
        var path = TempFile();
        try
        {
            string[] inputs = ["é", "日", "ab", "x", "ñ1", "1234567", "日本"];
            using (var w = new SimpleParquetWriter(path, recordsInRowGroup: 16))
            {
                foreach (var s in inputs)
                {
                    w.WriteField("s", s);
                    w.NextRecord();
                }
            }
            using var r = new ParquetFileReader(path);
            using var rg = r.RowGroup(0);
            var read = ReadCol<string>(rg, 0, inputs.Length);
            Assert.That(read, Is.EqualTo(inputs));
        }
        finally { TryDelete(path); }
    }

    [Test]
    public void RoundTrip_LongStringsViaDictionarySpanRowGroups()
    {
        var path = TempFile();
        try
        {
            var inputs = Enumerable.Range(0, 20)
                .Select(i => "longstring_" + new string('x', 5 + i))
                .ToArray();

            using (var w = new SimpleParquetWriter(path, recordsInRowGroup: 4))
            {
                foreach (var s in inputs)
                {
                    w.WriteField("s", s);
                    w.NextRecord();
                }
            }

            using var r = new ParquetFileReader(path);
            Assert.That(r.FileMetaData.NumRowGroups, Is.EqualTo(5));

            var collected = new List<string>();
            for (int g = 0; g < r.FileMetaData.NumRowGroups; g++)
            {
                using var rg = r.RowGroup(g);
                int n = (int)rg.MetaData.NumRows;
                collected.AddRange(ReadCol<string>(rg, 0, n));
            }
            Assert.That(collected, Is.EqualTo(inputs));
        }
        finally { TryDelete(path); }
    }

    [Test]
    public void RoundTrip_BoundaryRowCounts([Values(1, 63, 64, 65, 128, 129)] int rowCount)
    {
        var path = TempFile();
        try
        {
            using (var w = new SimpleParquetWriter(path, recordsInRowGroup: 64))
            {
                for (int i = 0; i < rowCount; i++)
                {
                    w.WriteField("i", i);
                    w.NextRecord();
                }
            }

            using var r = new ParquetFileReader(path);
            int total = 0;
            var values = new List<int>();
            for (int g = 0; g < r.FileMetaData.NumRowGroups; g++)
            {
                using var rg = r.RowGroup(g);
                int n = (int)rg.MetaData.NumRows;
                values.AddRange(ReadCol<int>(rg, 0, n));
                total += n;
            }
            Assert.That(total, Is.EqualTo(rowCount));
            Assert.That(values, Is.EqualTo(Enumerable.Range(0, rowCount)));
        }
        finally { TryDelete(path); }
    }

    [Test]
    public void RoundTrip_ExtremeValues()
    {
        var path = TempFile();
        try
        {
            using (var w = new SimpleParquetWriter(path, recordsInRowGroup: 8))
            {
                w.WriteField("i32", int.MinValue);
                w.WriteField("i64", long.MinValue);
                w.WriteField("u64", ulong.MaxValue);
                w.WriteField("nan", double.NaN);
                w.WriteField("pinf", double.PositiveInfinity);
                w.WriteField("ninf", double.NegativeInfinity);
                w.NextRecord();

                w.WriteField("i32", int.MaxValue);
                w.WriteField("i64", long.MaxValue);
                w.WriteField("u64", 0UL);
                w.WriteField("nan", 0.0);
                w.WriteField("pinf", -0.0);
                w.WriteField("ninf", 1e-300);
                w.NextRecord();
            }

            using var r = new ParquetFileReader(path);
            using var rg = r.RowGroup(0);
            var i32 = ReadCol<int>(rg, 0, 2);
            var i64 = ReadCol<long>(rg, 1, 2);
            var u64 = ReadCol<ulong>(rg, 2, 2);
            var nan = ReadCol<double>(rg, 3, 2);
            var pinf = ReadCol<double>(rg, 4, 2);
            var ninf = ReadCol<double>(rg, 5, 2);

            Assert.That(i32[0], Is.EqualTo(int.MinValue));
            Assert.That(i32[1], Is.EqualTo(int.MaxValue));
            Assert.That(i64[0], Is.EqualTo(long.MinValue));
            Assert.That(i64[1], Is.EqualTo(long.MaxValue));
            Assert.That(u64[0], Is.EqualTo(ulong.MaxValue));
            Assert.That(u64[1], Is.EqualTo(0UL));
            Assert.That(double.IsNaN(nan[0]));
            Assert.That(double.IsPositiveInfinity(pinf[0]));
            Assert.That(double.IsNegativeInfinity(ninf[0]));
        }
        finally { TryDelete(path); }
    }

    [Test]
    public void EmptyWriter_NoFileWritten()
    {
        var path = TempFile();
        TryDelete(path);
        using (var _ = new SimpleParquetWriter(path)) { }
        Assert.That(File.Exists(path), Is.False);
    }

    private static void TryDelete(string path)
    {
        try { File.Delete(path); } catch { }
    }
}