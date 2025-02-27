namespace Bendo.SimpleDataFrameBuilder.Tests;

public class Tests
{
    [Test]
    public void SmokeTests()
    {
        using var dfw = new SimpleParquetWriter("test.parquet", recordsInRowGroup: 64);

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
            dfw.NextRecord();
        }
    }
}