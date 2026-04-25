using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ParquetSharp;

namespace Bendo.SimpleDataFrameBuilder;

public sealed class SimpleParquetWriter : IDisposable
{
    public static Action<string> Logger = s => Console.WriteLine(s);
    public static bool VerifyColumnOrder { get; set; }

    private readonly string _filename;
    private readonly int _recordsInRowGroup;
    private readonly bool _zstd;
    private readonly bool _append;

    private readonly BlockingCollection<ColumnSet> _writeQueue = new(1);
    private readonly BlockingCollection<ColumnSet> _freeQueue = new(2);
    private readonly Thread _writerThread;

    private readonly List<Column> _columns = [];
    private readonly List<int> _colIndices = [];
    private readonly List<string> _colsGotNow = [];

    private readonly ColumnSet[] _allSets = [new ColumnSet(), new ColumnSet()];
    private ColumnSet _active;

    private bool _firstRowSeen;
    private bool _writerThreadStarted;
    private bool _disposed;
    private int _colIx;
    private int _dataIx;
    private ParquetFileWriter? _parquetWriter;

    public SimpleParquetWriter(string filename, int recordsInRowGroup = 65536, bool zstd = true, bool append = false)
    {
        _filename = filename;
        _recordsInRowGroup = recordsInRowGroup;
        _zstd = zstd;
        _append = append;

        _active = _allSets[0];
        _freeQueue.Add(_allSets[1]);

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

    public void WriteField<T>(string name, T val, int idx = -1)
    {
        ThrowIfDisposed();
        if (!_firstRowSeen)
        {
            if (idx >= 0)
            {
                name = string.Format(name, idx.ToString());
            }

            AddNewColumn<T>(name);
            AddValue(val);
            return;
        }

        if (VerifyColumnOrder)
        {
            if (idx >= 0)
            {
                name = string.Format(name, idx.ToString());
            }
            _colsGotNow.Add(name);
        }

        AddValue(val);
    }

    public void WriteFieldFmt<T>(string name, T val, string fmt)
    {
        ThrowIfDisposed();
        if (!_firstRowSeen)
        {
            name = string.Format(name, fmt);

            AddNewColumn<T>(name);
            AddValue(val);
            return;
        }

        if (VerifyColumnOrder)
        {
            name = string.Format(name, fmt);
            _colsGotNow.Add(name);
        }

        AddValue(val);
    }

    public void NextRecord()
    {
        ThrowIfDisposed();
        if (!_firstRowSeen)
        {
            _firstRowSeen = true;
            OpenParquetWriter();
            _writerThreadStarted = true;
            _writerThread.Start();
        }
        else if (VerifyColumnOrder)
        {
            CheckColsGotNow();
            _colsGotNow.Clear();
        }

        _colIx = 0;
        ++_dataIx;

        if (_dataIx >= _recordsInRowGroup)
        {
            FlushRowGroup();
            _dataIx = 0;
        }
    }

    private void OpenParquetWriter()
    {
        if (!_append && File.Exists(_filename))
        {
            Logger($"Deleting {_filename} as we are rewriting it.");
            File.Delete(_filename);
        }

        var compression = _zstd ? Compression.Zstd : Compression.Snappy;
        var builder = new WriterPropertiesBuilder().Compression(compression);
        foreach (var column in _columns)
        {
            if (column.LogicalSystemType == typeof(double) || column.LogicalSystemType == typeof(float))
            {
                builder.DisableDictionary(column.Name);
            }
        }

        _parquetWriter = new ParquetFileWriter(_filename, _columns.ToArray(), builder.Build());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddValue<T>(T val)
    {
        if (_colIx >= _colIndices.Count)
        {
            throw new InvalidOperationException(
                $"Wrote more fields than declared columns ({_colIndices.Count})");
        }
        int colArrIx = _colIndices[_colIx];
        int dataIx = _dataIx;
        var set = _active;

        if (typeof(T) == typeof(string))
        {
            var str = val as string;
            if (str == null)
            {
                ThrowHelperNull(_columns[_colIx].Name);
            }
            set.Strings[colArrIx][dataIx] = str!;
        }
        else if (typeof(T) == typeof(int))
        {
            if (val is int i32) set.Ints[colArrIx][dataIx] = i32;
        }
        else if (typeof(T) == typeof(double))
        {
            if (val is double d64) set.Doubles[colArrIx][dataIx] = d64;
        }
        else if (typeof(T) == typeof(float))
        {
            if (val is float d32) set.Floats[colArrIx][dataIx] = d32;
        }
        else if (typeof(T) == typeof(decimal))
        {
            if (val is decimal dec) set.Doubles[colArrIx][dataIx] = (double)dec;
        }
        else if (typeof(T) == typeof(bool))
        {
            if (val is bool b) set.Bools[colArrIx][dataIx] = b;
        }
        else if (typeof(T) == typeof(DateTime))
        {
            if (val is DateTime dt)
            {
                if (dt.Kind != DateTimeKind.Utc)
                {
                    throw new Exception($"{_columns[_colIx].Name} was not UTC!");
                }
                set.DateTimes[colArrIx][dataIx] = (dt.Ticks - UnixEpochTicks) / 10;
            }
        }
        else if (typeof(T) == typeof(long))
        {
            if (val is long i64) set.Longs[colArrIx][dataIx] = i64;
        }
        else if (typeof(T) == typeof(ulong))
        {
            if (val is ulong ui64) set.Ulongs[colArrIx][dataIx] = ui64;
        }
        else if (typeof(T) == typeof(short))
        {
            if (val is short i16) set.Shorts[colArrIx][dataIx] = i16;
        }
        else
        {
            throw new NotSupportedException($"{typeof(T).FullName} is not supported");
        }

        ++_colIx;
    }

    private void AddNewColumn<T>(string name)
    {
        CheckDupeColumn(name);

        if (typeof(T) == typeof(string))
        {
            _columns.Add(new Column(typeof(string), name));
            AddNewColIx(s => s.Strings, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(int))
        {
            _columns.Add(new Column(typeof(int), name));
            AddNewColIx(s => s.Ints, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(double))
        {
            _columns.Add(new Column(typeof(double), name));
            AddNewColIx(s => s.Doubles, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(float))
        {
            _columns.Add(new Column(typeof(float), name));
            AddNewColIx(s => s.Floats, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(decimal))
        {
            _columns.Add(new Column(typeof(double), name));
            AddNewColIx(s => s.Doubles, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(bool))
        {
            _columns.Add(new Column(typeof(bool), name));
            AddNewColIx(s => s.Bools, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(DateTime))
        {
            _columns.Add(new Column(typeof(DateTime), name));
            AddNewColIx(s => s.DateTimes, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(long))
        {
            _columns.Add(new Column(typeof(long), name));
            AddNewColIx(s => s.Longs, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(ulong))
        {
            _columns.Add(new Column(typeof(ulong), name));
            AddNewColIx(s => s.Ulongs, _recordsInRowGroup);
        }
        else if (typeof(T) == typeof(short))
        {
            _columns.Add(new Column(typeof(short), name));
            AddNewColIx(s => s.Shorts, _recordsInRowGroup);
        }
        else
        {
            throw new NotSupportedException($"{typeof(T).FullName} is not supported");
        }
    }

    private void AddNewColIx<T>(Func<ColumnSet, List<T[]>> selector, int n)
    {
        _colIndices.Add(selector(_allSets[0]).Count);
        foreach (var set in _allSets)
        {
            selector(set).Add(new T[n]);
        }
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

    private void FlushRowGroup()
    {
        _active.Count = _dataIx;
        _writeQueue.Add(_active);
        _active = _freeQueue.Take();
    }

    private void WriterLoop(object? _)
    {
        while (true)
        {
            ColumnSet set;
            try
            {
                set = _writeQueue.Take();
            }
            catch (InvalidOperationException)
            {
                if (_writeQueue is { IsAddingCompleted: true, Count: 0 })
                {
                    break;
                }
                throw;
            }

            WriteRowGroup(set);
            ClearReferences(set);
            _freeQueue.Add(set);
        }

        Logger($"Leaving writer loop for {_filename}");
    }

    private static void ClearReferences(ColumnSet set)
    {
        foreach (var arr in set.Strings)
        {
            Array.Clear(arr);
        }
    }

    private void WriteRowGroup(ColumnSet set)
    {
        using var groupWriter = _parquetWriter!.AppendRowGroup();
        int doubleColIx = 0, floatColIx = 0, intColIx = 0, shortColIx = 0;
        int longColIx = 0, ulongColIx = 0, dtColIx = 0, strColIx = 0, boolColIx = 0;
        int n = set.Count;

        foreach (var column in _columns)
        {
            if (column.LogicalSystemType == typeof(double))
            {
                WriteBatchPhysical<double, double>(groupWriter, set.Doubles[doubleColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(int))
            {
                WriteBatchPhysical<int, int>(groupWriter, set.Ints[intColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(long))
            {
                WriteBatchPhysical<long, long>(groupWriter, set.Longs[longColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(float))
            {
                WriteBatchPhysical<float, float>(groupWriter, set.Floats[floatColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(bool))
            {
                WriteBatchPhysical<bool, bool>(groupWriter, set.Bools[boolColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(ulong))
            {
                WriteBatchPhysical<ulong, long>(groupWriter, set.Ulongs[ulongColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(short))
            {
                WriteBatchPhysical<int, int>(groupWriter, set.Shorts[shortColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(string))
            {
                WriteBatchLogical(groupWriter, set.Strings[strColIx++], n);
            }
            else if (column.LogicalSystemType == typeof(DateTime))
            {
                WriteBatchPhysical<long, long>(groupWriter, set.DateTimes[dtColIx++], n);
            }
            else
            {
                throw new NotSupportedException($"{column.LogicalSystemType} not supported");
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteBatchPhysical<TArr, TPhys>(RowGroupWriter gw, TArr[] values, int n)
        where TArr : unmanaged
        where TPhys : unmanaged
    {
        using var cw = (ColumnWriter<TPhys>)gw.NextColumn();
        var span = MemoryMarshal.Cast<TArr, TPhys>(values.AsSpan(0, n));
        cw.WriteBatch(n, ReadOnlySpan<short>.Empty, ReadOnlySpan<short>.Empty, span);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteBatchLogical<T>(RowGroupWriter gw, T[] values, int n)
    {
        using var lw = gw.NextColumn().LogicalWriter<T>();
        lw.WriteBatch(values.AsSpan(0, n));
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_dataIx > 0)
        {
            _active.Count = _dataIx;
            _writeQueue.Add(_active);
        }
        _writeQueue.CompleteAdding();

        if (_writerThreadStarted)
        {
            Logger($"Waiting for writer thread for {_filename}");
            _writerThread.Join();
        }

        _parquetWriter?.Dispose();
        if (_parquetWriter != null)
        {
            Logger($"Disposed parquet writer for {_filename}");
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SimpleParquetWriter));
        }
    }

    private const long UnixEpochTicks = 621355968000000000L;

    private sealed class ColumnSet
    {
        public readonly List<double[]> Doubles = [];
        public readonly List<float[]> Floats = [];
        public readonly List<int[]> Ints = [];
        public readonly List<int[]> Shorts = [];
        public readonly List<long[]> Longs = [];
        public readonly List<ulong[]> Ulongs = [];
        public readonly List<long[]> DateTimes = [];
        public readonly List<string[]> Strings = [];
        public readonly List<bool[]> Bools = [];
        public int Count;
    }
}
