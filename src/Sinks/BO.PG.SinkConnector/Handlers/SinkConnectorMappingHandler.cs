using BO.Core;
using BO.Core.Entities;
using BO.Core.Interfaces;
using BO.PG.SourceConnector.Models;

namespace BO.PG.SinkConnector.Handlers;

public class SinkConnectorMappingHandler : ISinkConnectorMappingHandler<CreatePGDestConnector>
{
	public async Task<(Destination Destination, TaskRun TaskRun)> HandlAsync(CreatePGDestConnector input)
	{
		var destination = new Destination
		{
			Name = input.Name,
		};

		var appConfiguration = new PgAppConfiguration
		{
			Topics = string.IsNullOrEmpty(input.Topics) ? null :  input.Topics.Split(","),
			TopicPattern = string.IsNullOrEmpty(input.TopicPattern) ? null : input.TopicPattern,
			Schema = input.Schema,
			Consumer = input.Consumer,
			ConnectionString = input.ConnectionString,
		};

		var taskRun = new TaskRun
		{
			Id = destination.Id,
			ReferenceId = destination.Id,
			Name = input.Name,
			AppName = Constants.AppNames.POSTGRESQL,
			Type = Core.Entities.Enums.TaskRunType.Dest
		};

		destination.AppConfiguration = appConfiguration.Serialize();

		return (destination, taskRun);
	}
}
