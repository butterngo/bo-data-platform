using System.Text.Json.Serialization;

namespace BO.Core.Models;

public class ColumnDescriptor
{
	[JsonPropertyName("field")]
	public string? Field { get; set; }
	[JsonPropertyName("isPrimary")]
	public bool IsPrimary { get; set; }
	[JsonPropertyName("type")]
	public string? Type { get; set; }
	[JsonPropertyName("isNullable")]
	public bool IsNullable { get; set; }
}

public abstract class TableSchemaBase
{
	public TableSchemaBase() { }

	[JsonIgnore]
	public string Topic => $"bo_connector_{TableSchame}_{TableName}_topic";
	[JsonIgnore]
	public virtual string QualifiedName => $"{TableSchame}.{TableName}";
	[JsonPropertyName("id")]
	public string Id { get; set; } = Guid.NewGuid().ToString();
	[JsonPropertyName("tableSchame")]
	public string TableSchame { get; set; }
	[JsonPropertyName("tableName")]
	public string TableName { get; set; }
	
	[JsonPropertyName("columnDescriptors")]
	public IEnumerable<ColumnDescriptor> ColumnDescriptors { get; set; }

	public TableSchemaBase(string tableSchame, string tableName, IEnumerable<ColumnDescriptor> columnDescriptors)
	{
		TableSchame = tableSchame;

		TableName = tableName;

		ColumnDescriptors = columnDescriptors;
	}

	public string GetStrColumnName() => string.Join(", ", ColumnDescriptors.Select(x => x.Field));
}
