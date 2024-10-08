using BO.Core.Extensions;

namespace BO.Core.Entities;

/// <summary>
/*
CREATE TABLE bo_connectors.destinations (
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
public class Destination: EntityBase
{
	public required string Name { get; set; }
	public string? AppConfiguration { get; set; }
	public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

	public static string Insert = @"INSERT INTO bo_connectors.destinations
(id, name, app_configuration, created_at)
VALUES(@id, @name, @appConfiguration::json, NOW());";
}

public static class DestinationSchema
{
	public static string Table => $"{nameof(Destination)}s".ToSnakeCase();

	public static class Columns
	{
		public static string Id { get; } = nameof(Destination.Id).ToSnakeCase();
		public static string Name { get; } = nameof(Destination.Name).ToSnakeCase();
		public static string AppConfiguration { get; } = nameof(Destination.AppConfiguration).ToSnakeCase();
		public static string CreatedAt { get; } = nameof(Destination.CreatedAt).ToSnakeCase();
	}
}