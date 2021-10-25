using System.Buffers;
using System.Runtime.InteropServices;
using revtask.Core;

namespace revtask.Helpers;

public struct WaitAndActJob<T> : IJob, IJobExecuteOnCondition
    where T : IJob
{
    private readonly JobRequest[] _handles;
    private readonly int _handleCount;

    private readonly JobRequest[]? _tracked;

    public readonly T Batch;

    private readonly bool _trackNewBatch;

    public WaitAndActJob(T batch, ReadOnlySpan<JobRequest> handle, bool trackNewBatch = true)
    {
        _handles = ArrayPool<JobRequest>.Shared.Rent(handle.Length);
        _handleCount = handle.Length;
        
        handle.CopyTo(_handles.AsSpan(0, _handleCount));

        Batch = batch;

        if ((_trackNewBatch = trackNewBatch) == true)
        {
            _tracked = ArrayPool<JobRequest>.Shared.Rent(1);
        }
        else
        {
            _tracked = null;
        }
    }

    public WaitAndActJob(T batch, JobRequest handle, bool trackNewBatch = true)
        : this(batch, MemoryMarshal.CreateSpan(ref handle, 1), trackNewBatch)
    {
    }

    public int SetupJob(JobSetupInfo info)
    {
        return 1;
    }

    public void Execute(IJobRunner runner, JobExecuteInfo info)
    {
    }

    public bool CanExecute(IJobRunner runner, JobExecuteInfo info)
    {
        if (_trackNewBatch && _tracked![0] != default)
        {
            if (!runner.IsCompleted(_tracked[0])) 
                return false;
            
            ArrayPool<JobRequest>.Shared.Return(_tracked);
            return true;
        }

        foreach (var handle in _handles.AsSpan(0, _handleCount))
        {
            if (!runner.IsCompleted(handle))
            {
                return false;
            }
        }
        
        ArrayPool<JobRequest>.Shared.Return(_handles);

        if (_trackNewBatch)
        {
            _tracked![0] = runner.Queue(Batch);
            return false;
        }

        return true;
    }
}