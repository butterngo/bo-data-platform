using BO.Core.Models;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace BO.Worker.Models;

public class BigQueryAppConfiguration : AppConfiguration
{
	[JsonPropertyName("connectionString")]
	public required string ConnectionString { get; set; }
	[JsonPropertyName("schema")]
	public required string Schema { get; set; }
	[JsonPropertyName("topics")]
	public IEnumerable<string>? Topics { get; set; }

	[JsonPropertyName("topicPattern")]
	public string? TopicPattern { get; set; }

	public override string Serialize() => JsonSerializer.Serialize(this);
}
