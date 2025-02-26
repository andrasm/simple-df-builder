using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ParquetSharp;
using Encoding = System.Text.Encoding;

namespace Bendo.SimpleParquetWriter;

public sealed class SimpleParquetWriter : IDisposable
{
    public static Action<string> Logger = s => Console.WriteLine(s);
    private readonly string _filename;

    private readonly int _recordsInRowGroup;

    private readonly bool _zstd;
    private readonly bool _append;

    private readonly BlockingCollection<(byte[] data, int cursor, string[])> _writeQueue = new(1);
    private readonly Thread _writerThread;

    private readonly List<Column> _columns = [];

    private readonly Dictionary<string, int> _stringsDic = [];
    private readonly List<string> _stringsArr = [];
    private string[] _stringsArrCopy = [];
    private readonly List<double[]> _doubleCols = [];
    private readonly List<float[]> _floatCols = [];
    private readonly List<int[]> _intCols = [];
    private readonly List<short[]> _shortCols = [];
    private readonly List<long[]> _longCols = [];
    private readonly List<DateTime[]> _dateCols = [];
    private readonly List<string[]> _stringCols = [];
    private readonly List<bool[]> _boolCols = [];
    private readonly List<int> _colIndices = [];

    private byte[] _active;

    private bool _firstRowSeen;
    private int _colIx = 0;
    private int _dataIx = 0;
    private ParquetFileWriter? _parquetWriter;

    public SimpleParquetWriter(string filename, int recordsInRowGroup = 65536, bool zstd = true, bool append = false)
    {
        IsActive = true;
        _filename = filename;
        _recordsInRowGroup = recordsInRowGroup;
        _zstd = zstd;
        _append = append;
        
        var file = Path.GetFileNameWithoutExtension(_filename);
        _writerThread = new Thread(WriterLoop)
        {
            Name = $"PW_{file}",
            IsBackground = false
        };

#if DEBUG
        VerifyColumnOrder = true;
#endif
    }

    private void AddNewColIx<T>(List<T[]> dataArr)
    {
        _colIndices.Add(dataArr.Count);
        dataArr.Add(new T[_recordsInRowGroup]);
    }

    private const byte STRING_IN_DIC = 255;
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddStringData(string str)
    {
        var slot = _active.AsSpan().Slice(_cursor << 3, 8);
        if (str.Length <= 7 && Encoding.UTF8.GetByteCount(str) <= 7)
        {
            slot[0] = (byte)str.Length;
            var written = Encoding.UTF8.GetBytes(str, slot.Slice(1));
            if (written > 7)
            {
                throw new InvalidOperationException("didn't expect to write more than 8 bytes");
            }
            return;
        }

        slot[0] = STRING_IN_DIC;
        
        if (!_stringsDic.TryGetValue(str, out var ix))
        {
            ix = _stringsDic.Count;
            _stringsDic[str] = ix;
            _stringsArr.Add(str);
        }

        BitConverter.TryWriteBytes(slot.Slice(4), ix);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string ReadString(ReadOnlySpan<byte> data, string[] stringsArr)
    {
        if (data[0] == STRING_IN_DIC)
        {
            var ix = BitConverter.ToInt32(data.Slice(4));
            return stringsArr[ix];
        }

        int len = data[0];
        return Encoding.UTF8.GetString(data.Slice(1, len));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddData<T>(List<T[]> dataArr, in T data)
    {
        dataArr[_colIndices[_colIx]][_dataIx] = data;
    }

    private readonly List<string> _colsGotNow = [];

    public void WriteField(string name, double val, int idx = -1)
    {
        WriteField<double>(name, val, idx);
    }

    public void WriteField(string name, float val, int idx = -1)
    {
        WriteField<float>(name, val, idx);
    }

    private int _cursor = 0;
    private bool _writerThreadStarted;

    public void WriteField<T>(string name, T val, int idx = -1)
    {
        if (!_firstRowSeen)
        {
            if (idx >= 0)
            {
                name = string.Format(name, idx.ToString());
            }
            
            CheckDupeColumn(name);

            if (typeof(T) == typeof(decimal))
            {
                _columns.Add(new Column(typeof(double), name));
            }
            //else if (typeof(T) == typeof(DateTime))
            //{
            //    _columns.Add(new DateTimeDataField(name, DateTimeFormat.DateAndTime));
            //}
            else
            {
                _columns.Add(new Column(typeof(T), name));
            }

            if (typeof(T) == typeof(string))
            {
                AddNewColIx(_stringCols);
            }
            else if (typeof(T) == typeof(Int32))
            {
                AddNewColIx(_intCols);
            }
            else if (typeof(T) == typeof(double))
            {
                AddNewColIx(_doubleCols);
            }
            else if (typeof(T) == typeof(float))
            {
                AddNewColIx(_floatCols);
            }
            else if (typeof(T) == typeof(decimal))
            {
                AddNewColIx(_doubleCols);
            }
            else if (typeof(T) == typeof(bool))
            {
                AddNewColIx(_boolCols);
            }
            else if (typeof(T) == typeof(DateTime))
            {
                AddNewColIx(_dateCols);
            }
            else if (typeof(T) == typeof(long))
            {
                AddNewColIx(_longCols);
            }
            else if (typeof(T) == typeof(short))
            {
                AddNewColIx(_shortCols);
            }
            else
                throw new NotSupportedException($"{typeof(T).FullName} is not supported");
            
            if (typeof(T) == typeof(string))
            {
                var str = val as string;
                if (str == null)
                {
                    ThrowHelperNull(name);
                }
                AddData(_stringCols, str);
            }
            else if (typeof(T) == typeof(Int32))
            {
                if (val is int i32)
                {
                    AddData(_intCols, i32);
                }
            }
            else if (typeof(T) == typeof(double))
            {
                if (val is double d64)
                {
                    AddData(_doubleCols, d64);
                }
            }
            else if (typeof(T) == typeof(float))
            {
                if (val is float d32)
                {
                    AddData(_floatCols, d32);
                }
            }
            else if (typeof(T) == typeof(decimal))
            {
                if (val is decimal dec)
                {
                    AddData(_doubleCols, (double)dec);
                }
            }
            else if (typeof(T) == typeof(bool))
            {
                if (val is bool b)
                {
                    AddData(_boolCols, b);
                }
            }
            else if (typeof(T) == typeof(DateTime))
            {
                if (val is DateTime dt)
                {
                    if (dt.Kind != DateTimeKind.Utc)
                    {
                        throw new Exception($"{name} was not UTC!");
                    }

                    AddData(_dateCols, dt);
                }
            }
            else if (typeof(T) == typeof(long))
            {
                if (val is long i64)
                {
                    AddData(_longCols, i64);
                }
            }
            else if (typeof(T) == typeof(short))
            {
                if (val is short i16)
                {
                    AddData(_shortCols, i16);
                }
            }
            else
                throw new NotSupportedException($"{typeof(T).FullName} is not supported");

            ++_colIx;
            
            return;
        }
        else if (VerifyColumnOrder)
        {
            if (idx >= 0)
            {
                name = string.Format(name, idx.ToString());
            }

            _colsGotNow.Add(name);
        }

        if (typeof(T) == typeof(string))
        {
            var str = val as string;
            AddStringData(str);
        }
        else if (typeof(T) == typeof(Int32))
        {
            if (val is int i32)
            {
                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), i32);
            }
        }
        else if (typeof(T) == typeof(double))
        {
            if (val is double d64)
            {
                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), d64);
            }
        }
        else if (typeof(T) == typeof(float))
        {
            if (val is float d32)
            {
                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), d32);
            }
        }
        else if (typeof(T) == typeof(decimal))
        {
            if (val is decimal dec)
            {
                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), (double)dec);
            }
        }
        else if (typeof(T) == typeof(bool))
        {
            if (val is bool b)
            {
                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), b);
            }
        }
        else if (typeof(T) == typeof(DateTime))
        {
            if (val is DateTime dt)
            {
                if (dt.Kind != DateTimeKind.Utc)
                {
                    throw new Exception($"{name} was not UTC!");
                }

                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), DateTime.SpecifyKind(dt, DateTimeKind.Unspecified).Ticks);
            }
        }
        else if (typeof(T) == typeof(long))
        {
            if (val is long i64)
            {
                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), i64);
            }
        }
        else if (typeof(T) == typeof(short))
        {
            if (val is short i16)
            {
                BitConverter.TryWriteBytes(_active.AsSpan().Slice(_cursor << 3, 8), i16);
            }
        }
        else
            throw new NotSupportedException($"{typeof(T).FullName} is not supported");

        ++_colIx;
        ++_cursor;
    }

    private static void ThrowHelperNull(string name)
    {
        throw new InvalidOperationException($"{name} field was null");
    }

    private void CheckDupeColumn(string name)
    {
        if (_columns.Any(c => c.Name == name))
        {
            throw new InvalidOperationException($"duplicate column: {name}");
        }
    }


    public bool IsActive { get; }

    public void NextRecord()
    {
        var dataSize = _recordsInRowGroup * _columns.Count * 8;
        if (!_firstRowSeen)
        {
            _active = ArrayPool<byte>.Shared.Rent(dataSize);
            _firstRowSeen = true;

            CopyFirstRow();

            if (!_append && File.Exists(_filename))
            {
                Logger($"deleting file as we are rewriting it: {_filename}");
                File.Delete(_filename);
            }

            var compression = Compression.Snappy;
            if (_zstd)
            {
                compression = Compression.Zstd;
            }

            var builder = new WriterPropertiesBuilder()
                .Compression(compression);

            foreach (var column in _columns)
            {
                if (column.LogicalSystemType == typeof(double) || column.LogicalSystemType == typeof(float))
                {
                    builder.DisableDictionary(column.Name);
                }
            }
            
            _parquetWriter = new ParquetFileWriter(_filename, _columns.ToArray(), builder.Build());
            _writerThreadStarted = true;
            _writerThread.Start();
        }
        else
        {
            if (VerifyColumnOrder)
            {
                CheckColsGotNow();
                _colsGotNow.Clear();
            }
        }

        _colIx = 0;
        ++_dataIx;

        if (_dataIx >= _recordsInRowGroup)
        {
            AddToWriter();
            _dataIx = 0;
            _active = ArrayPool<byte>.Shared.Rent(dataSize);
            _cursor = 0;
        }
    }

    private void AddToWriter()
    {
        if (_stringsArrCopy.Length != _stringsArr.Count)
        {
            _stringsArrCopy = _stringsArr.ToArray();
        }
        _writeQueue.Add((_active, _cursor, _stringsArrCopy));
    }

    private void WriterLoop(object obj)
    {
        while (true)
        {
            byte[] data;
            int cursor;
            string[] stringsArr;
            try
            {
                (data, cursor, stringsArr) = _writeQueue.Take();
            }
            catch (InvalidOperationException e)
            {
                if (_writeQueue.IsAddingCompleted && _writeQueue.Count == 0)
                {
                    break;    
                }

                throw;
            }
            CopyToLists(data, cursor, stringsArr);
            ArrayPool<byte>.Shared.Return(data, clearArray: true);
            var dataCount = cursor / _columns.Count;
            WriteRowGroup(dataCount);
        }
        
        Logger("exiting writer loop.");
    }
    
    private void CopyToLists(byte[] active, int cursor, string[] stringsArr)
    {
        var data = active.AsSpan();
        for (int c = 0; c < _columns.Count; c++)
        {
            var column = _columns[c];
            var colIndex = _colIndices[c];
            var stride = _columns.Count * 8;
            int dix = 0;
            int len = cursor * 8;
            if (column.LogicalSystemType == typeof(double))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _doubleCols[colIndex][dix++] = BitConverter.ToDouble(data.Slice(i, 8));
                }
            }
            else if (column.LogicalSystemType == typeof(int))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _intCols[colIndex][dix++] = BitConverter.ToInt32(data.Slice(i, 8));
                }
            }
            else if (column.LogicalSystemType == typeof(long))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _longCols[colIndex][dix++] = BitConverter.ToInt64(data.Slice(i, 8));
                }
            }
            else if (column.LogicalSystemType == typeof(short))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _shortCols[colIndex][dix++] = BitConverter.ToInt16(data.Slice(i, 8));
                }
            }
            else if (column.LogicalSystemType == typeof(string))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _stringCols[colIndex][dix++] = ReadString(data.Slice(i, 8), stringsArr);
                }
            }
            else if (column.LogicalSystemType == typeof(DateTime))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _dateCols[colIndex][dix++] = new DateTime(BitConverter.ToInt64(data.Slice(i, 8)));
                }
            }
            else if (column.LogicalSystemType == typeof(float))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _floatCols[colIndex][dix++] = BitConverter.ToSingle(data.Slice(i, 8));
                }
            }
            else if (column.LogicalSystemType == typeof(bool))
            {
                for (int i = c * 8; i < len; i += stride)
                {
                    _boolCols[colIndex][dix++] = BitConverter.ToBoolean(data.Slice(i, 8));
                }
            }
            else
            {
                throw new NotSupportedException($"{column.LogicalSystemType} not supported");
            }
        }
    }

    private void CopyFirstRow()
    {
        for (var i = 0; i < _columns.Count; i++)
        {
            var column = _columns[i];
            if (column.LogicalSystemType == typeof(double))
            {
                WriteField(column.Name, _doubleCols[_colIndices[i]][0]);
            }
            else if (column.LogicalSystemType == typeof(int))
            {
                WriteField(column.Name, _intCols[_colIndices[i]][0]);
            }
            else if (column.LogicalSystemType == typeof(long))
            {
                WriteField(column.Name, _longCols[_colIndices[i]][0]);
            }
            else if (column.LogicalSystemType == typeof(short))
            {
                WriteField(column.Name, _shortCols[_colIndices[i]][0]);
            }
            else if (column.LogicalSystemType == typeof(string))
            {
                WriteField(column.Name, _stringCols[_colIndices[i]][0]);
            }
            else if (column.LogicalSystemType == typeof(DateTime))
            {
                WriteField(column.Name, _dateCols[_colIndices[i]][0]);
            }
            else if (column.LogicalSystemType == typeof(float))
            {
                WriteField(column.Name, _floatCols[_colIndices[i]][0]);
            }
            else if (column.LogicalSystemType == typeof(bool))
            {
                WriteField(column.Name, _boolCols[_colIndices[i]][0]);
            }
            else
            {
                throw new NotSupportedException($"{column.LogicalSystemType} not supported");
            }
        }

        _colsGotNow.Clear();
    }

    public static bool VerifyColumnOrder { get; set; }

    private void CheckColsGotNow()
    {
        if (_columns.Count != _colsGotNow.Count)
        {
            var missingCols = _columns.Where(c => !_colsGotNow.Contains(c.Name)).Select(c => c.Name).ToList();
            var missingCols2 = _colsGotNow.Where(c => _columns.All(c2 => c2.Name != c)).ToList();
            throw new InvalidOperationException(
                $"cols expected: {_columns.Count}, columns got: {_colsGotNow.Count}, missing1: {string.Join(", ", missingCols)}, missing2: {string.Join(", ", missingCols2)}");
        }

        for (var i = 0; i < _columns.Count; i++)
        {
            var expected = _columns[i];
            var actual = _colsGotNow[i];
            if (expected.Name != actual)
            {
                throw new InvalidOperationException(
                    $"Column[{i}] diff, expected: {expected.Name}, actual: {actual}");
            }
        }
    }

    private void WriteRowGroup(int dataCount = -1)
    {
        using var groupWriter = _parquetWriter.AppendRowGroup();
        int intColIx = 0;
        int longColIx = 0;
        int shortColIx = 0;
        int floatColIx = 0;
        int strColIx = 0;
        int boolColIx = 0;
        int doubleColIx = 0;
        int dtColIx = 0;
        for (var i = 0; i < _columns.Count; i++)
        {
            var column = _columns[i];
            if (column.LogicalSystemType == typeof(double))
            {
                WriteBatch(groupWriter, _doubleCols, ref doubleColIx, dataCount);
            }
            else if (column.LogicalSystemType == typeof(int))
            {
                WriteBatch(groupWriter, _intCols, ref intColIx, dataCount);
            }
            else if (column.LogicalSystemType == typeof(long))
            {
                WriteBatch(groupWriter, _longCols, ref longColIx, dataCount);
            }
            else if (column.LogicalSystemType == typeof(short))
            {
                WriteBatch(groupWriter, _shortCols, ref shortColIx, dataCount);
            }
            else if (column.LogicalSystemType == typeof(string))
            {
                WriteBatch(groupWriter, _stringCols, ref strColIx, dataCount);
            }
            else if (column.LogicalSystemType == typeof(DateTime))
            {
                WriteBatch(groupWriter, _dateCols, ref dtColIx, dataCount);
            }
            else if (column.LogicalSystemType == typeof(float))
            {
                WriteBatch(groupWriter, _floatCols, ref floatColIx, dataCount);
            }
            else if (column.LogicalSystemType == typeof(bool))
            {
                WriteBatch(groupWriter, _boolCols, ref boolColIx, dataCount);
            }
            else
            {
                throw new NotSupportedException($"{column.LogicalSystemType} not supported");
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteBatch<T>(RowGroupWriter groupWriter, List<T[]> dataArr, ref int ix, int dataCount = -1)
    {
        using var lw = groupWriter.NextColumn().LogicalWriter<T>();
        var values = dataArr[ix++];
        if (dataCount < 0)
        {
            lw.WriteBatch(values);
        }
        else
        {
            lw.WriteBatch(values.AsSpan().Slice(0, dataCount));
        }
    }

    public void Dispose()
    {
        if (_dataIx > 0)
        {
            // write last rowgroup
            AddToWriter();
        }
        _writeQueue.CompleteAdding();

        if (_writerThreadStarted)
        {
            Logger("waiting for writer thread");
        
            _writerThread.Join();
        }
        
        _parquetWriter?.Dispose();
        if (_parquetWriter != null)
        {
            Logger($"Disposed parquet writer for {_filename}");
        }
    }
}