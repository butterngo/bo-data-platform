using BO.Core;
using BO.Core.Interfaces;
using BO.Core.Entities.Enums;

namespace BO.Worker;

internal class TaskRunFactory : ITaskRunFactory
{
	private readonly IEnumerable<ITaskRunHandler> _taskRunHandlers;

	private Dictionary<string, Type> Dic { get; set; } = new();

	public TaskRunFactory(IEnumerable<ITaskRunHandler> taskRunHandlers)
	{
		_taskRunHandlers = taskRunHandlers;
		SetSrcConnector();
		SetDestConnector();
	}

	private void SetDestConnector() 
	{
		Dic.Add($"{Constants.AppNames.BIGQUERY}_{TaskRunType.Dest}", typeof(BigQuery.SinkConnector.Handlers.TaskRunBigQueryHandler));
		Dic.Add($"{Constants.AppNames.POSTGRESQL}_{TaskRunType.Dest}", typeof(PG.SinkConnector.Handlers.TaskRunPostgresqlHandler));
	}

	private void SetSrcConnector() 
	{
		Dic.Add($"{Constants.AppNames.POSTGRESQL}_{TaskRunType.Src}", typeof(PG.SourceConnector.Handlers.TaskRunPostgresqlHandler));
	}

	public ITaskRunHandler GetHander(string name, TaskRunType taskRunType)
	{
		var key = $"{name}_{taskRunType}";

		if (!Dic.ContainsKey(key))
		{
			throw new NotImplementedException(key);
		}

		var handler = _taskRunHandlers.FirstOrDefault(x => x.GetType().Equals(Dic[key]));

		if (handler == null)
		{
			throw new NotImplementedException(Dic[key].FullName);
		}

		return handler;
	}
}
