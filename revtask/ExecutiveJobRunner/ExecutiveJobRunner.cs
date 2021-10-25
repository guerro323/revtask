using revtask.Core;

namespace revtask.ExecutiveJobRunner;

public class ExecutiveJobRunner : IJobRunner
{
    public bool IsCancelled()
    {
        return false;
    }

    public bool IsWarmed()
    {
        return true;
    }

    public bool IsCompleted(JobRequest request)
    {
        return true;
    }

    public JobRequest Queue<T>(T batch) where T : IJob
    {
        batch.Execute(this, default);
        return default;
    }

    public void TryDivergeRequest(JobRequest request)
    {
    }

    public void TryDiverge()
    {
    }
}