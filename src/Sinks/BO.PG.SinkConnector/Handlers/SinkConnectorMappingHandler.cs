using BO.Core;
using BO.Core.Entities;
using BO.Core.Implementations;
using BO.Core.Interfaces;
using BO.Core.Models;
using BO.PG.SourceConnector.Models;

namespace BO.PG.SinkConnector.Handlers;

internal class SinkConnectorMappingHandler : DestinationConnectorMappingBaseHandler<CreatePGDestConnector>
{
	

	protected override Task<Destination> GetDestinationAsync(CreatePGDestConnector input, CancellationToken cancellationToken)
	{
		var destination = new Destination
		{
			Name = input.Name,
			AppConfiguration = new PgAppConfiguration
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
