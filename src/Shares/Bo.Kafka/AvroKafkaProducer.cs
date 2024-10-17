using Avro;
using Avro.Generic;
using Bo.Kafka.Models;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using static System.Net.Mime.MediaTypeNames;

namespace Bo.Kafka;

public class AvroKafkaProducer : IDisposable
{
	private readonly IProducer<Null, GenericRecord> _producer;

	private readonly CachedSchemaRegistryClient _schemaRegistry;

	public AvroKafkaProducer(KafkaOptions kafkaOptions)
	{

		var schemaRegistryConfig = new SchemaRegistryConfig
		{
			Url = "http://localhost:8081"
		};

		_schemaRegistry = new CachedSchemaRegistryClient(kafkaOptions.SchemaRegistryConfig);

		var avroSerializerConfig = new AvroSerializerConfig
		{
			AutoRegisterSchemas = true
		};

		_producer = new ProducerBuilder<Null, GenericRecord>(kafkaOptions.ProducerConfig)
			.SetValueSerializer(new AvroSerializer<GenericRecord>(_schemaRegistry, avroSerializerConfig))
			.Build();
	}

	public async Task<DeliveryResult<Null, GenericRecord>> ProduceAsync(string topic, KafkaMessageGenerator message, CancellationToken cancellationToken = default(CancellationToken))
	{
		message.schema.fields.Add(new KafkaMessageField("op", "string"));
		message.schema.fields.Add(new KafkaMessageField("ts_ms", "long"));
		
		var record = new GenericRecord(message.schema.GenerateAvroSchema());
		record.Add("op", message.op);
		record.Add("ts_ms", message.ts_ms);

		foreach (var field in message.payload)
		{
			record.Add(field.Key, field.Value);
		}
		
		return await _producer.ProduceAsync(topic, new Message<Null, GenericRecord> 
		{
			Value = record
		}, cancellationToken);
	}

	public void Dispose()
	{
		_producer.Dispose();
		_schemaRegistry.Dispose();
	}
}
