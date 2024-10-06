using BO.Core.Entities.Enums;
using BO.Core.Extensions;

namespace BO.Core.Entities;

/// <summary>
/*
CREATE TABLE bo_connectors.sources (
	id text NOT NULL,
	connection_string text NOT NULL,
	pg_publication_name varchar(200) NOT NULL,
	pg_slot_name varchar(200) NOT NULL,
	schema_type int2 NOT NULL,
    publisher_type int2 NOT NULL,
	json_publisher json NOT NULL,
	json_table json NOT NULL,
	created_at timestamp,
	UNIQUE (pg_publication_name),
	CONSTRAINT pk_sources  PRIMARY KEY (id)
)
 */
///https://debezium.io/documentation/reference/stable/connectors/mysql.html
/// </summary>
public class Source
{
	public string Id { get; set; } = Guid.NewGuid().ToString();
	public required string ConnectionString { get; set; }
	private string _pgPublicationName = string.Empty;
	private string _pgSlotName = string.Empty;
	public required string PgPublicationName 
	{
		get => _pgPublicationName; 
		set => _pgPublicationName = value.ToLower();
	}
	public required string PgSlotName 
	{
		get => _pgSlotName;
		set => _pgSlotName = value.ToLower();
	}
	public JsonSchemaType SchemaType { get; set; }
	public PublisherType PublisherType { get; set; }
	public string? JsonTable { get; set; }
	public required string JsonPublisher { get; set; }
	public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

	public static string Insert = @"INSERT INTO bo_connectors.sources
(id, connection_string, pg_publication_name, pg_slot_name, schema_type, publisher_type, json_publisher, json_table, created_at)
VALUES(@id, @connectionString, @pgPublicationName, @pgSlotName, 0, 0, @jsonPublisher::json, @jsonTable::json, NOW());";
}

public static class SourceSchema 
{
	public static string Table => $"{nameof(Source)}s".ToSnakeCase();

	public static class Columns
	{
		public static string Id { get; } = nameof(Source.Id).ToSnakeCase();
		public static string ConnectionString { get; } = nameof(Source.ConnectionString).ToSnakeCase();
		public static string SchemaType { get; } = nameof(Source.SchemaType).ToSnakeCase();
		public static string PublisherType { get; } = nameof(Source.PublisherType).ToSnakeCase();
		public static string JsonTable { get; } = nameof(Source.JsonTable).ToSnakeCase();
		public static string JsonPublisher { get; } = nameof(Source.JsonPublisher).ToSnakeCase();
		public static string CreatedAt { get; } = nameof(Source.CreatedAt).ToSnakeCase();
	}
}
