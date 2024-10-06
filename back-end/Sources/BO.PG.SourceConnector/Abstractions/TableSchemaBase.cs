using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace BO.PG.SourceConnector.Abstractions;

public class ColumnDescriptor
{
	[JsonPropertyName("field")]
	public string? Field { get; set; }
	[JsonPropertyName("type")]
	public string? Type { get; set; }
	[JsonPropertyName("isNullable")]
	public bool IsNullable { get; set; }
}

public abstract class TableSchemaBase
{
	public TableSchemaBase() { }

	[JsonIgnore]
	public string Topic => $"bo_pg_source_connector_{TableSchame}_{TableName}";
	[JsonIgnore]
	public virtual string QualifiedName => $"{TableSchame}.{TableName}";
	[JsonPropertyName("id")]
	public string Id { get; set; } = Guid.NewGuid().ToString();
	[JsonPropertyName("tableSchame")]
	public string TableSchame { get; set; }
	[JsonPropertyName("tableName")]
	public string TableName { get; set; }
	[JsonPropertyName("connectionString")]
	public string ConnectionString { get; set; }

	[JsonPropertyName("columnDescriptors")]
	public IEnumerable<ColumnDescriptor> ColumnDescriptors { get; set; }

	protected abstract IEnumerable<ColumnDescriptor> GetColumnDescriptors(List<Dictionary<string, string>> dic);

	public TableSchemaBase(string tableSchame, string tableName, string connectionString, string jsonColumn)
	{
		TableSchame = tableSchame;

		TableName = tableName;

		ConnectionString = connectionString;

		ColumnDescriptors = GetColumnDescriptors(Deserialize(jsonColumn));
	}

	public string GetStrColumnName() => string.Join(", ", ColumnDescriptors.Select(x => x.Field));

	private static List<Dictionary<string, string>> Deserialize(string json)
	{
		var columnDescriptors = new List<Dictionary<string, string>>();

		var nodes = JsonNode.Parse(json).AsArray();

		foreach (var node in nodes)
		{
			columnDescriptors.Add(JsonSerializer.Deserialize<Dictionary<string, string>>(node.GetValue<string>()));
		}

		return columnDescriptors;
	}
}
