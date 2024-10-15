using BO.Core;
using BO.Core.Entities.Enums;
using BO.Core.Models;

namespace BO.BigQuery.SinkConnector.Models;

public class CreateBigQueryDestConnector : ConnectorBaseModel
{
	public override string AppName => Constants.AppNames.BIGQUERY;

	public required string Schema { get; set; }
	public string? Topics { get; set; }
	public string? TopicPattern { get; set; }
	public required string ConnectionString { get; set; }
	public JsonSchemaType SchemaType { get; set; }
	public string? KafkaServer { get; set; }

	public Dictionary<string, object> Consumer => new Dictionary<string, object>
	{
		{ "groupId", $"bo_bq_sink_connector-{Schema}".ToLower() },
		{ "kafkaServer", KafkaServer }
	};
}
