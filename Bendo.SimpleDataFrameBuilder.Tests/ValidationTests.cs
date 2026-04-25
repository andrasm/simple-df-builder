namespace Bendo.SimpleDataFrameBuilder.Tests;

public class ValidationTests
{
    private static string TempFile() =>
        Path.Combine(Path.GetTempPath(), $"sdf_test_{Guid.NewGuid():N}.parquet");

    private bool _origVerify;

    [SetUp]
    public void SetUp()
    {
        _origVerify = SimpleParquetWriter.VerifyColumnOrder;
    }

    [TearDown]
    public void TearDown()
    {
        SimpleParquetWriter.VerifyColumnOrder = _origVerify;
    }

    [Test]
    public void DuplicateColumnName_Throws()
    {
        var path = TempFile();
        try
        {
            using var w = new SimpleParquetWriter(path);
            w.WriteField("x", 1);
            Assert.Throws<InvalidOperationException>(() => w.WriteField("x", 2));
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Test]
    public void VerifyColumnOrder_MissingColumn_Throws()
    {
        SimpleParquetWriter.VerifyColumnOrder = true;
        var path = TempFile();
        try
        {
            using var w = new SimpleParquetWriter(path);
            w.WriteField("a", 1);
            w.WriteField("b", 2);
            w.NextRecord();
            w.WriteField("a", 3);
            Assert.Throws<InvalidOperationException>(() => w.NextRecord());
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Test]
    public void ExtraColumn_Throws()
    {
        var path = TempFile();
        try
        {
            using var w = new SimpleParquetWriter(path);
            w.WriteField("a", 1);
            w.NextRecord();
            w.WriteField("a", 2);
            Assert.Throws<InvalidOperationException>(() => w.WriteField("b", 3));
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Test]
    public void VerifyColumnOrder_ReorderedColumns_Throws()
    {
        SimpleParquetWriter.VerifyColumnOrder = true;
        var path = TempFile();
        try
        {
            using var w = new SimpleParquetWriter(path);
            w.WriteField("a", 1);
            w.WriteField("b", 2);
            w.NextRecord();
            w.WriteField("b", 3);
            w.WriteField("a", 4);
            Assert.Throws<InvalidOperationException>(() => w.NextRecord());
        }
        finally { try { File.Delete(path); } catch { } }
    }

    [Test]
    public void UnsupportedType_Throws()
    {
        var path = TempFile();
        try
        {
            using var w = new SimpleParquetWriter(path);
            Assert.Throws<NotSupportedException>(() => w.WriteField("g", Guid.NewGuid()));
        }
        finally { try { File.Delete(path); } catch { } }
    }
}