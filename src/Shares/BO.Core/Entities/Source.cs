using BO.Core.Extensions;

namespace BO.Core.Entities;

/// <summary>
/*
CREATE TABLE IF NOT EXISTS bo_connectors.sources (
	id text NOT NULL,
	name text NOT NULL,
	app_configuration text NOT NULL,
	created_at timestamp,
	UNIQUE (name),
	CONSTRAINT pk_sources  PRIMARY KEY (id)
)
 */
///https://debezium.io/documentation/reference/stable/connectors/mysql.html
/// </summary>
public class Source: EntityBase
{
	public required string Name { get; set; }
	
	public string? AppConfiguration { get; set; }
	
	public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

	public static string Insert = @"INSERT INTO bo_connectors.sources
(id, name, app_configuration, created_at)
VALUES(@id, @name, @appConfiguration::json, NOW());";
}

public static class SourceSchema 
{
	public static string Table => $"{nameof(Source)}s".ToSnakeCase();

	public static class Columns
	{
		public static string Id { get; } = nameof(Source.Id).ToSnakeCase();
		public static string Name { get; } = nameof(Source.Name).ToSnakeCase();
		public static string AppConfiguration { get; } = nameof(Source.AppConfiguration).ToSnakeCase();
		public static string CreatedAt { get; } = nameof(Source.CreatedAt).ToSnakeCase();
	}
}
