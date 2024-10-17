using Bo.Kafka.Models;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;

namespace Bo.Kafka;

public class KafkaConsumer : IDisposable
{
	private readonly IConsumer<Ignore, KafkaMessageGenerator> _consumer;
	private readonly CachedSchemaRegistryClient _schemaRegistry;

	public KafkaConsumer(ConsumerConfig config) 
	{
		var schemaRegistryConfig = new SchemaRegistryConfig
		{
			Url = "http://localhost:8081"
		};

		_schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
		var avroDeserializerConfig = new AvroDeserializerConfig();

		config.EnableAutoCommit = false;
		var consumerBuilder = new ConsumerBuilder<Ignore, KafkaMessageGenerator>(config);
		//consumerBuilder.SetValueDeserializer(KafkaMessageSerializer<KafkaMessage>.Create());
		consumerBuilder.SetValueDeserializer(new AvroDeserializer<KafkaMessageGenerator>(_schemaRegistry, avroDeserializerConfig).AsSyncOverAsync());
		consumerBuilder.SetLogHandler((_, logHandler) => { });
		consumerBuilder.SetErrorHandler((_, errorHandler) => { });
		_consumer = consumerBuilder.Build();
	}

	public async Task Consume(string regexTopic, Func<KafkaMessageGenerator, Task> func, CancellationToken cancellationToken)
	{
		while (!cancellationToken.IsCancellationRequested)
		{
			_consumer.Subscribe(regexTopic);

			var consumeResult = _consumer.Consume(cancellationToken);

			if (consumeResult != null) 
			{
				await func(consumeResult.Message.Value);
			}

			_consumer.Commit(consumeResult);
		}
	}

	public async Task Consume(IEnumerable<string> topics, Func<KafkaMessageGenerator, Task> func, CancellationToken cancellationToken) 
	{
		while (!cancellationToken.IsCancellationRequested)
		{
			_consumer.Subscribe(topics);
			
			var consumeResult = _consumer.Consume(cancellationToken);

			if (consumeResult != null)
			{
				await func(consumeResult.Message.Value);
			}

			_consumer.Commit(consumeResult);
		}
	}

	public void Dispose()
	{
		_consumer.Close();
		_schemaRegistry.Dispose();
	}
}
