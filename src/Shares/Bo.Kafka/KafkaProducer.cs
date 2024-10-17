using Bo.Kafka.Models;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Bo.Kafka;

public class KafkaProducer : IDisposable
{
	private readonly IProducer<Null, KafkaMessageGenerator> _producer;
	private readonly CachedSchemaRegistryClient _schemaRegistry;

	public KafkaProducer(ProducerConfig config)
	{
		var schemaRegistryConfig = new SchemaRegistryConfig
		{
			Url = "http://localhost:8081"
		};

		_schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

		var avroSerializerConfig = new AvroSerializerConfig
		{
			AutoRegisterSchemas = true
		};

		_producer = new ProducerBuilder<Null, KafkaMessageGenerator>(config)
			//.SetValueSerializer(KafkaMessageSerializer<KafkaMessage>.Create())
			.SetValueSerializer(new AvroSerializer<KafkaMessageGenerator>(_schemaRegistry, avroSerializerConfig))
			.Build();
	}

	public async Task<DeliveryResult<Null, KafkaMessageGenerator>> ProduceAsync(string topic, KafkaMessageGenerator message, CancellationToken cancellationToken = default(CancellationToken))
	{
		return await _producer.ProduceAsync(topic, new Message<Null, KafkaMessageGenerator> { Value = message }, cancellationToken);
	}

	public void Dispose()
	{
		_producer.Dispose();
		_schemaRegistry.Dispose();
	}
}

