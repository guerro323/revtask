using revghost.Shared.Threading;
using revtask.Core;
using revtask.Core.Internal;

namespace revtask.OpportunistJobRunner;

public partial class OpportunistJobRunner
{
    private abstract class BatchTypeCollectionTypedBase<T> : BatchTypeCollectionBase
    {
        public BusySynchronizationManager _dictionarySynchronizer = new();
        public T[] _queuedBatchesMap = Array.Empty<T>();

        public readonly OpportunistJobRunner _owner;

        public BatchTypeCollectionTypedBase(OpportunistJobRunner owner) => _owner = owner;

        public void Enqueue(T batch, JobRequest request)
        {
            _dictionarySynchronizer.Lock();
            if (request.Id >= _queuedBatchesMap.Length)
            {
                Array.Resize(ref _queuedBatchesMap, _owner._batches.Versions.Length);
            }

            _dictionarySynchronizer.Unlock(true);

            _queuedBatchesMap[request.Id] = batch;
        }
    }

    private static BatchTypeCollectionTypedBase<T> CreateCollection<T>(OpportunistJobRunner runner)
        where T : IJob
    {
        if (BatchTypeUtility<T>.IsCompletion || BatchTypeUtility<T>.IsCondition)
            return new BatchTypeCollectionMixed<T>(runner);

        return new BatchTypeCollectionSimple<T>(runner);
    }

    private class BatchTypeCollectionMixed<T> : BatchTypeCollectionTypedBase<T>
        where T : IJob
    {
        public override bool TryExecute(JobRequest request, int index, int task)
        {
            var batch = _queuedBatchesMap[request.Id];

            var execInfo = new JobExecuteInfo(request, index, task, _owner._tasks.Length);

            if (!BatchTypeUtility<T>.CanExecute(batch, _owner, execInfo))
            {
                return false;
            }

            Exception exception = null;
            try
            {
                batch.Execute(_owner, execInfo);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            ref readonly var batches = ref _owner._batches;
            using (batches.ResultSynchronization.Synchronize())
            {
                ref var result = ref _owner._batches.Results[request.Id];
                // Check if the next increment will be MaxIndex - 1 (the -1 is a hack to make the OnComplete increment it to the max and to make sure that it's not completed from the outside)
                if (Interlocked.Increment(ref result.SuccessfulWrite) == request.IndexCount
                    && BatchTypeUtility<T>.IsCompletion)
                {
                    BatchTypeUtility<T>.OnComplete(batch, _owner, exception);

                    // Increment it
                    Interlocked.Increment(ref result.SuccessfulWrite);
                }
                else
                {
                    if (exception != null)
                        Console.WriteLine(exception);
                }

                if (result.IsCompleted)
                {
                    batches.SetUnused(request.Id);
                }
            }

            return true;
        }

        public BatchTypeCollectionMixed(OpportunistJobRunner owner) : base(owner)
        {
        }
    }

    private class BatchTypeCollectionSimple<T> : BatchTypeCollectionTypedBase<T>
        where T : IJob
    {
        public override bool TryExecute(JobRequest request, int index, int task)
        {
            var batch = _queuedBatchesMap[request.Id];

            var execInfo = new JobExecuteInfo(request, index, task, _owner._tasks.Length);
            batch.Execute(_owner, execInfo);

            ref readonly var batches = ref _owner._batches;

            batches.ResultSynchronization.Lock();
            ref var result = ref _owner._batches.Results[request.Id];

            if (Interlocked.Increment(ref result.SuccessfulWrite) >= result.MaxIndex)
            {
                batches.SetUnused(request.Id);
            }

            batches.ResultSynchronization.Unlock(true);

            return true;
        }

        public BatchTypeCollectionSimple(OpportunistJobRunner owner) : base(owner)
        {
        }
    }
}