namespace revtask.Core;

public interface IJob
{
    int SetupJob(JobSetupInfo info);
    void Execute(IJobRunner runner, JobExecuteInfo info);
}

public interface IJobExecuteOnCondition
{
    bool CanExecute(IJobRunner runner, JobExecuteInfo info);
}

public interface IJobExecuteOnComplete
{
    void OnComplete(IJobRunner runner, Exception? exception);
}

public interface IJobSetHandle
{
    void SetHandle(IJobRunner runner, JobRequest handle);
}

public record struct JobSetupInfo(int TaskCount);

public record struct JobExecuteInfo(JobRequest Request, int Index, int Task, int TaskCount)
{
    public int MaxUseIndex => Request.IndexCount;
}