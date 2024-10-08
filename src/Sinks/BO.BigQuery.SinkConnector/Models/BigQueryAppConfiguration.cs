using BO.Core.Models;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace BO.Worker.Models;

public class BigQueryAppConfiguration : AppConfiguration
{
	[JsonPropertyName("topics")]
	public required IEnumerable<string> Topics { get; set; }

	public override string Serialize() => JsonSerializer.Serialize(this);
}
