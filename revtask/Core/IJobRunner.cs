namespace revtask.Core;

public interface IJobRunner
{
    bool IsCancelled();
    
    bool IsWarmed();
    bool IsCompleted(JobRequest request);

    JobRequest Queue<T>(T batch) where T : IJob;

    void TryDivergeRequest(JobRequest request);
    void TryDiverge();
}

public record struct JobRequest(int Id, int Version, int IndexCount);

public static class BatchRunnerExtensions
{
    public static void QueueAndComplete<TRunner, TJob>(this TRunner runner, TJob job)
        where TRunner : IJobRunner
        where TJob : IJob
    {
        var req = runner.Queue(job);
        CompleteBatch(runner, req);
    }

    public static void CompleteBatch<TRunner>(this TRunner runner, JobRequest request, bool forceDiverge = true)
        where TRunner : IJobRunner
    {
        var iteration = 10;
        while (!runner.IsCancelled() && !runner.IsCompleted(request) && iteration-- > 0)
        {
            // First try diverging and complete the initial request
            runner.TryDivergeRequest(request);
        }

        if (runner.IsCompleted(request))
            return;

        // We couldn't diverge on caller thread, so let's wait without diverging
        iteration = 100;
        while (!runner.IsCompleted(request) && iteration-- > 0)
        {
            Thread.Sleep(0);
        }

        if (runner.IsCancelled() || runner.IsCompleted(request))
            return;

        if (!forceDiverge)
        {
            var iterSleep1 = 0;
            while (!runner.IsCancelled() && !runner.IsCompleted(request))
            {
                if (iteration-- <= 0)
                {
                    Thread.Sleep(0);
                    iteration = 10;
                    iterSleep1++;
                }

                if (iterSleep1 > 100)
                {
                    Thread.Sleep(1);
                    iterSleep1 = 0;
                }
            }

            return;
        }

        // We are forced to diverge on another thread...
        // this will alloc.
        ThreadPool.QueueUserWorkItem(static arg =>
        {
            // If it's not completed, then it can mean that it need to wait up on other tasks.
            // and if all threads are busy with diverging tasks, then it would result into a deadlock.
            // so accept the fate and diverge all requests into the worker thread.
            //
            // This would mostly happen if a task call 'CompleteBatch' or loop IsComplete()
            //
            // This can result into a deadlock if called on caller thread:
            // A wait B which wait C
            // B was started before A, and B call this method to wait for C.
            // C isn't yet completed, so it need to diverge all tasks
            // which call A, and then will never complete because B is waiting for C.

            var threadWait = new SpinWait();
            while (!arg.runner.IsCancelled() && !arg.runner.IsCompleted(arg.request))
            {
                arg.runner.TryDiverge();
                threadWait.SpinOnce();
            }
        }, (runner, request), false);

        while (!runner.IsCancelled() && !runner.IsCompleted(request))
        {
            Thread.Sleep(0);
        }
    }
}