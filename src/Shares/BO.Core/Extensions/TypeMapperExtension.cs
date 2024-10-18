using NpgsqlTypes;

namespace BO.Core.Extensions;

public static class TypeMapperExtension
{
	public static string MapNpgsqlDbTypeToAvroType(this NpgsqlDbType npgsqlDbType)
	{
		return npgsqlDbType switch
		{
			NpgsqlDbType.Boolean => "boolean",
			NpgsqlDbType.Smallint => "int",
			NpgsqlDbType.Integer => "int",
			NpgsqlDbType.Bigint => "long",
			NpgsqlDbType.Real => "float",
			NpgsqlDbType.Double => "double",
			NpgsqlDbType.Numeric => "double", // Avro does not have a decimal type, so double is used
			NpgsqlDbType.Text => "string",
			NpgsqlDbType.Varchar => "string",
			NpgsqlDbType.Char => "string",
			NpgsqlDbType.Json => "string",
			NpgsqlDbType.Jsonb => "string",
			NpgsqlDbType.Uuid => "string", // Avro does not have a UUID type, so string is used
			NpgsqlDbType.Bytea => "bytes",
			NpgsqlDbType.Timestamp => "long", // Avro uses long for timestamps (milliseconds since epoch)
			NpgsqlDbType.TimestampTz => "long", // Same as above
			_ => "string"
		};
	}
	public static Type MapAvroTypeToCSharpType(this string avroType)
	{
		return avroType switch
		{
			"null" => typeof(void),
			"boolean" => typeof(bool),
			"int" => typeof(int),
			"long" => typeof(long),
			"float" => typeof(float),
			"double" => typeof(double),
			"bytes" => typeof(byte[]),
			"string" => typeof(string),
			"record" => typeof(object), // Typically, you'd map this to a specific class
			"enum" => typeof(Enum), // Typically, you'd map this to a specific enum
			"array" => typeof(System.Collections.IList), // Or a more specific type like List<T>
			"map" => typeof(System.Collections.IDictionary), // Or a more specific type like Dictionary<string, T>
			"fixed" => typeof(byte[]),
			_ => throw new ArgumentException($"Unsupported Avro type: {avroType}")
		};
	}
}
