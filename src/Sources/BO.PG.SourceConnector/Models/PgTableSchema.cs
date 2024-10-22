using Avro;
using NpgsqlTypes;
using Avro.Generic;
using BO.Core.Models;
using BO.Core.Converters;
using BO.Core.Extensions;

namespace BO.PG.SourceConnector.Models;

public class PgTableSchema : TableSchemaBase
{ 
	public PgTableSchema() { }

	public PgTableSchema(string tableSchame, string tableName, IEnumerable<ColumnDescriptor> columnDescriptors)
		: base(tableSchame, tableName, columnDescriptors)
	{
	}

	public GenericRecord SerializeKafkaMessage(Dictionary<string, object> payload) 
	{
		const string operation_key = "_ct";
		var avroSchema = (RecordSchema)RecordSchema.Parse(JsonAvroSchema);

		var record = new GenericRecord(avroSchema);

		record.Add("op", payload[operation_key]);

		record.Add("ts_ms", DateTime.UtcNow);

		record.Add("table_changed", payload[operation_key]);

		foreach (var item in payload)
		{
			if (item.Key.Equals(operation_key))
			{
				continue;
			}
			if (avroSchema.TryGetField(item.Key, out var field))
			{
				record.Add(item.Key, TypeConverterHelper.ChangeType(item.Value, field));
			}
			else 
			{
				throw new InvalidOperationException($"not found field {item.Key} maybe schema changed, please check it.");
			}
		}

		return record;
	}

	protected override string GenerateAvroSchema()
		=> this.ConvertPgTableToAvroSchema(ColumnDescriptors =>
	{
		ColumnDescriptors.Add(new ColumnDescriptor { Field = "op", Type = "string" });
		ColumnDescriptors.Add(new ColumnDescriptor { Field = "ts_ms", Type = NpgsqlDbType.Timestamp.ToString() });
		ColumnDescriptors.Add(new ColumnDescriptor { Field = "table_changed", Type = "string", IsNullable = true });
	}, "BO.PG.SourceConnector.Avro.Convertor");
}
