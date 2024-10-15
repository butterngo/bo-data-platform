using BO.Worker.Models;
using BO.Core.Entities;
using BO.Core.Implementations;
using BO.BigQuery.SinkConnector.Models;

namespace BO.BigQuery.SinkConnector.Handlers;

internal class SinkConnectorMappingHandler : DestinationConnectorMappingBaseHandler<CreateBigQueryDestConnector>
{
	protected override Task<Destination> GetDestinationAsync(CreateBigQueryDestConnector input, CancellationToken cancellationToken)
	{
		var destination = new Destination
		{
			Name = input.Name,
			AppConfiguration = new BigQueryAppConfiguration
			{
				Topics = string.IsNullOrEmpty(input.Topics) ? null : input.Topics.Split(","),
				TopicPattern = string.IsNullOrEmpty(input.TopicPattern) ? null : input.TopicPattern,
				Schema = input.Schema,
				Consumer = input.Consumer,
				ConnectionString = input.ConnectionString,
			}.Serialize()
		};

		return Task.FromResult(destination);
	}
}
