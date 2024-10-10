namespace BO.Core.Interfaces;

public interface ITaskManagement : IDisposable
{
    void DoWork<TInput>(TInput model, Func<TInput, IServiceProvider, CancellationToken, Task> func, CancellationToken cancellationToken) where TInput : ITaskRun;

    public void Cancel(string taskId);

    public Task Completion { get; }
    Func<Task, ITaskRun> OnBeforeDispose { get; set; }
}
