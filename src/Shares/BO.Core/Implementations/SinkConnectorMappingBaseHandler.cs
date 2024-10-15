using BO.Core.Models;
using BO.Core.Entities;

namespace BO.Core.Implementations;

public abstract class SinkConnectorMappingBaseHandler<TModel>
	where TModel : ConnectorBaseModel
{
	public async Task<(Destination Destination, TaskRun TaskRun)> HandlAsync(TModel input, CancellationToken cancellationToken)
	{
		cancellationToken.ThrowIfCancellationRequested();

		var destination = await GetDestinationAsync(input, cancellationToken);

		var taskRun = await GetTaskRunAsync(destination, input, cancellationToken);

		return (destination, taskRun);
	}

	protected abstract Task<Destination> GetDestinationAsync(TModel input, CancellationToken cancellationToken);

	protected virtual Task<TaskRun> GetTaskRunAsync(Destination destination, TModel input, CancellationToken cancellationToken) 
	{
		cancellationToken.ThrowIfCancellationRequested();

		var taskRun = new TaskRun
		{
			Id = destination.Id,
			ReferenceId = destination.Id,
			Name = input.Name,
			AppName = input.AppName,
			Type = Entities.Enums.TaskRunType.Dest
		};

		return Task.FromResult(taskRun);
	}
}
