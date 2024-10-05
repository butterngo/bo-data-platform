using BO.PG.SourceConnector.Entities.Enums;
using System.Text.Json;

namespace BO.PG.SourceConnector.Dtos;

public class CreateConnectorDto
{
	public required string Name { get; set; }
	public required string ConnectionString { get; set; }
	public required string Schema { get; set; }
	public required string Tables { get; set; }
	public JsonSchemaType SchemaType { get; set; }
	public PublisherType PublisherType { get; set; }
	public string? KafkaServer { get; set; }

	public string JsonPublisher => JsonSerializer.Serialize(new { PublisherType, KafkaServer }, new JsonSerializerOptions
	{
		PropertyNamingPolicy = JsonNamingPolicy.CamelCase
	});

}

