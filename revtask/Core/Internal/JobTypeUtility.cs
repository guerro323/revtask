using System.Runtime.CompilerServices;

namespace revtask.Core.Internal;

public static class JobTypeUtility
{
    public static void SetHandleCore<T>(ref T batch, IJobRunner runner, JobRequest handle)
        where T : IJobSetHandle
    {
        batch.SetHandle(runner, handle);
    }

    public static bool CanExecuteCore<T>(in T batch, IJobRunner runner, JobExecuteInfo info)
        where T : IJobExecuteOnCondition
    {
        return batch.CanExecute(runner, info);
    }

    public static void OnCompleteCore<T>(in T batch, IJobRunner runner, Exception? exception)
        where T : IJobExecuteOnComplete
    {
        batch.OnComplete(runner, exception);
    }
}

public static class BatchTypeUtility<T>
{
    public static bool IsCompletion = typeof(T).GetInterfaces().Contains(typeof(IJobExecuteOnComplete));
    public static bool IsCondition = typeof(T).GetInterfaces().Contains(typeof(IJobExecuteOnCondition));
    public static bool IsSetHandle = typeof(T).GetInterfaces().Contains(typeof(IJobSetHandle));

    private static SetHandleWithoutBoxing __setHandle;
    private static CanExecuteWithoutBoxing __canExecute;
    private static OnCompleteWithoutBoxing __onComplete;

    private delegate void SetHandleWithoutBoxing(ref T target, IJobRunner runner, JobRequest handle);
    private delegate bool CanExecuteWithoutBoxing(in T target, IJobRunner runner, JobExecuteInfo info);
    private delegate void OnCompleteWithoutBoxing(in T target, IJobRunner runner, Exception? exception);

    static BatchTypeUtility()
    {
        if (IsCondition)
        {
            var method = typeof(JobTypeUtility).GetMethod(nameof(JobTypeUtility.CanExecuteCore))!.MakeGenericMethod(typeof(T));
            __canExecute = (CanExecuteWithoutBoxing) Delegate.CreateDelegate(typeof(CanExecuteWithoutBoxing), method);
        }
        
        if (IsCompletion)
        {
            var method = typeof(JobTypeUtility).GetMethod(nameof(JobTypeUtility.OnCompleteCore))!.MakeGenericMethod(typeof(T));
            __onComplete = (OnCompleteWithoutBoxing) Delegate.CreateDelegate(typeof(OnCompleteWithoutBoxing), method);
        }
        
        if (IsSetHandle)
        {
            var method = typeof(JobTypeUtility).GetMethod(nameof(JobTypeUtility.SetHandleCore))!.MakeGenericMethod(typeof(T));
            __setHandle = (SetHandleWithoutBoxing) Delegate.CreateDelegate(typeof(SetHandleWithoutBoxing), method);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SetHandle(ref T batch, IJobRunner runner, JobRequest handle)
    {
        if (IsSetHandle)
        {
            __setHandle(ref batch, runner, handle);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CanExecute(in T batch, IJobRunner runner, JobExecuteInfo info)
    {
        if (IsCondition)
        {
            return __canExecute(in batch, runner, info);
        }

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void OnComplete(in T batch, IJobRunner runner, Exception? exception)
    {
        if (IsCompletion)
        {
            __onComplete(in batch, runner, exception);
        }
    }
}