using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using revtask.Core;
using revtask.Core.Internal;

namespace revtask.OpportunistJobRunner;

public partial class OpportunistJobRunner : IJobRunner, IDisposable
{
    private readonly BatchCollection _batches;
    private readonly CancellationTokenSource _ccs = new();
    
    private readonly Thread[] _tasks;
    private readonly TaskState[] _taskStates;

    public OpportunistJobRunner(float corePercentile)
    {
        _batches = new BatchCollection(this);
        
        var coreCount = Math.Clamp((int) (Environment.ProcessorCount * corePercentile), 1,
            Environment.ProcessorCount);

        _tasks = new Thread[coreCount];
        _taskStates = new TaskState[coreCount];
        for (var i = 0; i < _tasks.Length; i++)
        {
            _tasks[i] = new Thread(RunTaskCore!);
            _tasks[i].Start(_taskStates[i] = new TaskState()
            {
                Runner = this,

                ProcessorId = -1,

                Token = _ccs.Token,
                TaskIndex = i,
                TaskCount = _tasks.Length,
                Batches = _batches
            });
        }
    }

    public bool IsCancelled()
    {
        return _ccs.IsCancellationRequested;
    }

    public bool IsWarmed()
    {
        return true;
    }

    public bool IsCompleted(JobRequest request)
    {
        return request.Id == 0 || _batches.Versions[request.Id] > request.Version || _batches.Results[request.Id].IsCompleted;
    }

    public JobRequest Queue<T>(T batch) where T : IJob
    {
        var use = batch.SetupJob(new JobSetupInfo(_tasks.Length));
        if (use <= 0)
            return default;

        return _batches.Enqueue(batch, use);
    }

    public void TryDivergeRequest(JobRequest request)
    {
        if (IsCompleted(request))
            return;

        _batches.DivergeFor(0, request);
    }

    public void TryDiverge()
    {
        _batches.ExecuteAll(0);
    }
    
    /// <summary>
    /// Convert half of the runner threads into low latency threads.
    /// The other half will be used for higher throughput.
    /// </summary>
    /// <remarks>
    /// Call <see cref="StopPerformanceCriticalSection"/> as soon as possible.
    /// The caller processor will never be in a critical context.
    /// </remarks>
    public void StartPerformanceCriticalSection()
    {
        var currentProcessor = Thread.GetCurrentProcessorId();

        // Half of the threads will be in low latency mode, while others will be in high throughput mode
        var maxCriticalThread = (_tasks.Length + 1) / 2;
        if (maxCriticalThread == 0)
            return;
        
        foreach (var state in _taskStates)
        {
            var processorId = Volatile.Read(ref state.ProcessorId);

            // this is needed in case there is a task on the same processor as the caller of this runner.
            // or else everything would block
            state.IsPerformanceCritical = processorId != default && processorId != currentProcessor;

            if (maxCriticalThread-- > 0)
                break;
        }
    }

    public void StopPerformanceCriticalSection()
    {
        foreach (var state in _taskStates) state.IsPerformanceCritical = false;
    }

    private record struct BatchIndex(Type Type, JobRequest Request, int Index);

    private class BatchCollection : GenericCollection<IJob, BatchTypeCollectionBase>
    {
        private IntRowCollection _batchRows;

        public BatchResult[] Results = Array.Empty<BatchResult>();
        public int[] Versions = Array.Empty<int>();

        public ConcurrentQueue<BatchIndex> Queued = new();

        private readonly OpportunistJobRunner _owner;

        public BusySynchronizationManager ResultSynchronization = new();
        
        public BatchCollection(OpportunistJobRunner owner)
        {
            _owner = owner;

            _batchRows = new IntRowCollection((_, next) =>
            {
                Array.Resize(ref Versions, next);
                using (ResultSynchronization.Synchronize())
                {
                    Array.Resize(ref Results, next);
                }
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetUnused(int id)
        {
            ResultSynchronization.Lock();
            _batchRows.TrySetUnusedRow(id);
            ResultSynchronization.Unlock();
        }

        public JobRequest Enqueue<T>(T batch, int count)
            where T : IJob
        {
            var collection = GetCollection<T>();

            JobRequest req;
            using (ResultSynchronization.Synchronize())
            {
                var row = _batchRows.CreateRow();
                req = new JobRequest(row, Versions[row]++ + 1, count);

                Results[row] = new BatchResult
                {
                    SuccessfulWrite = 0,
                    // Make sure that batch with OnComplete method has an additional index for it;
                    // It's used to make sure that when you call IsCompleted(...) it only return true if OnComplete has been called
                    MaxIndex = count + (BatchTypeUtility<T>.IsCompletion ? 1 : 0)
                };

                BatchTypeUtility<T>.SetHandle(ref batch, _owner, req);
            }

            Unsafe.As<BatchTypeCollectionTypedBase<T>>(collection).Enqueue(batch, req);

            for (var i = 0; i < req.IndexCount; i++) Queued.Enqueue(new BatchIndex(typeof(T), req, i));

            return req;
        }

        [ThreadStatic] private static List<BatchIndex> _executeAllRequeue;

        public void DivergeFor(int task, JobRequest request)
        {
            _executeAllRequeue ??= new List<BatchIndex>();
            
            var previousType = default(Type);
            var previousCollection = default(BatchTypeCollectionBase);
            while (true)
            {
                if (!Queued.TryDequeue(out var batchIndex))
                {
                    break;
                }

                if (batchIndex.Request.Id != request.Id)
                {
                    _executeAllRequeue.Add(batchIndex);
                    continue;
                }

                if (previousType != batchIndex.Type)
                {
                    previousType = batchIndex.Type;

                    if (!Dictionary.TryGetValue(batchIndex.Type, out previousCollection))
                        throw new InvalidOperationException("couldn't find " + batchIndex.Type);
                }

                if (previousCollection!.TryExecute(batchIndex.Request, batchIndex.Index, task))
                    continue;

                _executeAllRequeue.Add(batchIndex);
            }

            foreach (var toRequeue in _executeAllRequeue)
                Queued.Enqueue(toRequeue);

            _executeAllRequeue.Clear();
        }
        
        public int ExecuteAll(int task)
        {
            _executeAllRequeue ??= new List<BatchIndex>();
            
            var previousType = default(Type);
            var previousCollection = default(BatchTypeCollectionBase);

            var runCount = 0;
            do
            {
                if (!Queued.TryDequeue(out var batchIndex))
                {
                    break;
                }

                if (previousType != batchIndex.Type)
                {
                    previousType = batchIndex.Type;

                    if (!Dictionary.TryGetValue(batchIndex.Type, out previousCollection!))
                        throw new InvalidOperationException("couldn't find " + batchIndex.Type);
                }

                if (!previousCollection.TryExecute(batchIndex.Request, batchIndex.Index, task))
                {
                    _executeAllRequeue.Add(batchIndex);
                    continue;
                }

                runCount++;
            } while (true);

            {
                foreach (var toRequeue in _executeAllRequeue)
                    Queued.Enqueue(toRequeue);
            }

            _executeAllRequeue.Clear();

            return runCount;
        }

        protected override BatchTypeCollectionBase OnCollectionAdded<T>()
        {
            return CreateCollection<T>(_owner);
        }
    }

    private abstract class BatchTypeCollectionBase
    {
        public abstract bool TryExecute(JobRequest request, int index, int task);
    }

    private class TaskState
    {
        public BatchCollection Batches;

        public bool IsPerformanceCritical;
        public int ProcessorId;
        public OpportunistJobRunner Runner;
        public int TaskCount;
        public int TaskIndex;

        public CancellationToken Token;
    }

    private record struct ScheduledBatch(Type Type)
    {
        public void Invoke()
        {
            
        }
    }
    
    private struct BatchResult
    {
        public int SuccessfulWrite;
        public int MaxIndex;

        public bool IsCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => SuccessfulWrite >= MaxIndex;
        }
    }

    public void Dispose()
    {
        _batches.Dispose();
        _ccs.Cancel();
    }
}