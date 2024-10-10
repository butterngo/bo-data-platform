﻿using Npgsql;
using NpgsqlTypes;
using System.Text;
using BO.Core.Models;
using Bo.Kafka.Models;
using System.Text.Json.Nodes;

namespace BO.PG.SourceConnector.Models;

public class PgTableSchema : TableSchemaBase
{
	public PgTableSchema() { }
	public PgTableSchema(string tableSchame, string tableName, IEnumerable<ColumnDescriptor> columnDescriptors) 
		: base(tableSchame, tableName, columnDescriptors)
	{
	}

	protected static NpgsqlDbType ParseEnum(string value)
	{
		try
		{
			return (NpgsqlDbType)Enum.Parse(typeof(NpgsqlDbType), value, true);
		}
		catch 
		{
			return NpgsqlDbType.Unknown;
		}
	}

	public KafkaMessage SerializeKafkaMessage(string json)
	{
		var jObj = JsonObject.Parse(json);
		var kafkaMessage = new KafkaMessage
		{
			op = jObj["_ct"].GetValue<string>(),
			ts_ms = Convert.ToInt64(jObj["_mts"].GetValue<string>()),
			source = new Dictionary<string, object> 
			{
				{ "table", jObj["_tbl"].GetValue<string>() }
			}
		};

		foreach (var column in ColumnDescriptors)
		{
			switch (ParseEnum(column.Type))
			{
				case NpgsqlDbType.Bigint:
					{
						kafkaMessage.SetValue(column.Field, "int64", jObj[column.Field].GetValue<long>(), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Double:
					{
						kafkaMessage.SetValue(column.Field, "double", jObj[column.Field].GetValue<double>(), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Real:
					{
						kafkaMessage.SetValue(column.Field, "decimal", jObj[column.Field].GetValue<float>(), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Numeric:
				case NpgsqlDbType.Money:
					{
						kafkaMessage.SetValue(column.Field, "decimal", jObj[column.Field].GetValue<decimal>(), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Smallint:
					{
						kafkaMessage.SetValue(column.Field, "int32", jObj[column.Field].GetValue<int>(), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Bytea:
					{
						kafkaMessage.SetValue(column.Field, "string", jObj[column.Field].GetValue<string>(), column.IsNullable);
						break;
					}
				default:
					{
						kafkaMessage.SetValue(column.Field, "string", jObj[column.Field].GetValue<string>(), column.IsNullable);
						break;
					}
			}
		}

		return kafkaMessage;
	}

	public KafkaMessage SerializeKafkaMessage(object obj, string table)
	{
		var reader = obj as NpgsqlBinaryExporter;

		var kafkaMessage = new KafkaMessage
		{
			source = new Dictionary<string, object>
			{
				{ "table", table }
			}
		};

		T GetValue<T>(NpgsqlBinaryExporter reader, NpgsqlDbType? npgsqlDbType = null)
		{
			try
			{
				if (npgsqlDbType.HasValue)
				{
					return reader.Read<T>(npgsqlDbType.Value);
				}


				return reader.Read<T>();
			}
			catch
			{
				return default(T);
			}

		}

		foreach (var column in ColumnDescriptors)
		{
			switch (ParseEnum(column.Type))
			{
				case NpgsqlDbType.Bigint:
					{
						kafkaMessage.SetValue(column.Field, "int64", GetValue<long>(reader, NpgsqlDbType.Bigint), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Double:
					{
						kafkaMessage.SetValue(column.Field, "double", GetValue<double>(reader, NpgsqlDbType.Double), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Real:
					{
						kafkaMessage.SetValue(column.Field, "decimal", GetValue<float>(reader, NpgsqlDbType.Real), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Numeric:
				case NpgsqlDbType.Money:
					{
						kafkaMessage.SetValue(column.Field, "decimal", GetValue<decimal>(reader, NpgsqlDbType.Numeric), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Smallint:
					{
						kafkaMessage.SetValue(column.Field, "int32", GetValue<int>(reader, NpgsqlDbType.Smallint), column.IsNullable);
						break;
					}
				case NpgsqlDbType.Bytea:
					{
						var value = GetValue<byte[]>(reader, NpgsqlDbType.Bytea);

						kafkaMessage.SetValue(column.Field, "string", value == null ? string.Empty : Encoding.UTF8.GetString(value), column.IsNullable);
						break;
					}
				default:
					{
						kafkaMessage.SetValue(column.Field, "string", GetValue<string>(reader), column.IsNullable);
						break;
					}
			}
		}

		return kafkaMessage;
	}
}