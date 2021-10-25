using System.Buffers;
using System.Runtime.InteropServices;
using revtask.Core;

namespace revtask.Helpers;

public struct WaitJob : IJob, IJobExecuteOnCondition
{
    private JobRequest[] _handles;
    private int _handleCount;
    
    public WaitJob(ReadOnlySpan<JobRequest> handle)
    {
        _handles = ArrayPool<JobRequest>.Shared.Rent(handle.Length);
        _handleCount = handle.Length;
        
        handle.CopyTo(_handles.AsSpan(0, _handleCount));
    }

    public WaitJob(JobRequest handle) : this(MemoryMarshal.CreateSpan(ref handle, 1))
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
        foreach (var handle in _handles.AsSpan(0, _handleCount))
        {
            if (!runner.IsCompleted(handle))
            {
                return false;
            }
        }
        
        ArrayPool<JobRequest>.Shared.Return(_handles);
        return true;
    }
}

public static class WaitBatchExtensions
{
    public static JobRequest WaitBatches<TRunner>(this TRunner runner, ReadOnlySpan<JobRequest> span)
        where TRunner : IJobRunner
    {
        return runner.Queue(new WaitJob(span));
    }

    public static JobRequest WaitBatches<TRunner>(this TRunner runner, List<JobRequest> span)
        where TRunner : IJobRunner
    {
        return runner.Queue(new WaitJob(CollectionsMarshal.AsSpan(span)));
    }
}