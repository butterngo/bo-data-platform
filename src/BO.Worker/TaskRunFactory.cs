using BO.Core;
using BO.Core.Interfaces;
using BO.PG.SourceConnector.Handlers;
using BO.Worker.Handlers;

namespace BO.Worker;

internal class TaskRunFactory : ITaskRunFactory
{
	private readonly IEnumerable<ITaskRunHandler> _taskRunHandlers;

	private Dictionary<string, Type> Dic = new Dictionary<string, Type>();

	public TaskRunFactory(IEnumerable<ITaskRunHandler> taskRunHandlers)
	{
		_taskRunHandlers = taskRunHandlers;
		Dic.Add(Constants.AppNames.BIGQUERY, typeof(TaskRunBigQueryHandler));
		Dic.Add(Constants.AppNames.POSTGRESQL, typeof(TaskRunKafkaHandler));
	}

	public ITaskRunHandler GetHander(string name)
	{
		if (!Dic.ContainsKey(name))
		{
			throw new NotImplementedException(name);
		}

		var handler = _taskRunHandlers.FirstOrDefault(x => x.GetType().Equals(Dic[name]));

		if (handler == null)
		{
			throw new NotImplementedException(Dic[name].FullName);
		}

		return handler;
	}
}
