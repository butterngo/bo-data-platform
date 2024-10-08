using BO.Core.Entities.Enums;

namespace BO.PG.SourceConnector.Models;

public class CreatePGSrcConnector
{
	public required string Name { get; set; }
	public required string ConnectionString { get; set; }
	public required string Schema { get; set; }
	public required string Tables { get; set; }
	public JsonSchemaType SchemaType { get; set; }
	public PublisherType PublisherType { get; set; }
	public string? KafkaServer { get; set; }

	public Dictionary<string, object> Publisher => new Dictionary<string, object>
	{
		{ "publisherType", PublisherType },
		{ "kafkaServer", KafkaServer }
	};
}
