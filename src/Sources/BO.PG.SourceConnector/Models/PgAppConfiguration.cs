using BO.Core.Entities.Enums;
using BO.Core.Models;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace BO.PG.SourceConnector.Models;

internal class PgAppConfiguration : AppConfiguration
{
	[JsonPropertyName("topic")]
	public string? Topic { get; set; }
	[JsonPropertyName("connectionString")]
	public required string ConnectionString { get; set; }
	private string _pgPublicationName = string.Empty;
	private string _pgSlotName = string.Empty;
	[JsonPropertyName("publicationName")]
	public required string PublicationName
	{
		get => _pgPublicationName;
		set => _pgPublicationName = value.ToLower();
	}
	[JsonPropertyName("slotName")]
	public required string SlotName
	{
		get => _pgSlotName;
		set => _pgSlotName = value.ToLower();
	}
	[JsonPropertyName("schemaType")]
	public JsonSchemaType SchemaType { get; set; }
	[JsonPropertyName("publisherType")]
	public PublisherType PublisherType { get; set; }
	[JsonPropertyName("tables")]
	public List<PgTableSchema> Tables { get; set; } = new();

	public override string Serialize() => JsonSerializer.Serialize(this);
}
