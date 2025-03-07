namespace Bendo.SimpleDataFrameBuilder.Tests;

public class Tests
{
    [Test]
    public void SmokeTests()
    {
        using var dfw = new SimpleParquetWriter("test.parquet", recordsInRowGroup: 64);

        string[] suffixes = ["a", "b", "c", "d", "e"];

        for (int i = 0; i < 130; i++)
        {
            dfw.WriteField("float", 1.0f);
            dfw.WriteField("double", 1.0);
            dfw.WriteField("int", 1);
            dfw.WriteField("long", 1L);
            dfw.WriteField("string", "hello");
            dfw.WriteField("bool", true);
            dfw.WriteField("ulong", 1UL);
            dfw.WriteField("datetime", DateTime.UtcNow);

            for (int j = 0; j < 5; j++)
            {
                dfw.WriteField("multiple_{0}", 10 - j, j);
            }

            for (int j = 0; j < 5; j++)
            {
                dfw.WriteFieldFmt("string_fmt_{0}", 10 - j, suffixes[j]);
            }

            dfw.NextRecord();
        }
    }

    [Test]
    public void UtcDateTime()
    {
        using var dfw = new SimpleParquetWriter("test2.parquet", recordsInRowGroup: 64);
        var exception = Assert.Throws<Exception>(() => dfw.WriteField("my_col", DateTime.Now));
        Assert.AreEqual("my_col was not UTC!", exception.Message);
    }
}