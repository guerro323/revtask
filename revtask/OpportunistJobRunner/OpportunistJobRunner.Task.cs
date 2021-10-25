namespace revtask.OpportunistJobRunner;

public partial class OpportunistJobRunner
{
    private static void RunTaskCore(object obj)
    {
        if (obj is not TaskState state)
            throw new InvalidOperationException($"Task {obj} is not a {typeof(TaskState)}");

        Thread.CurrentThread.Priority = ThreadPriority.AboveNormal;

        var spin = new SpinWait();
        var sleep0Threshold = 1000;
        var sleepCount = 0;

        while (!state.Runner.IsCancelled())
        {
            Volatile.Write(ref state.ProcessorId, Thread.GetCurrentProcessorId());

            int runCount;
            try
            {
                runCount = state.Batches.ExecuteAll(state.TaskIndex);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }

            if (state.IsPerformanceCritical)
            {
                if (runCount > 0)
                    sleepCount = 0;

                if (sleepCount++ > sleep0Threshold)
                {
                    Thread.Sleep(0);
                    sleepCount = 0;
                }

                continue;
            }

            spin.SpinOnce();
        }
    }
}