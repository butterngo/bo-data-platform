using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.Admin;

namespace Bo.Kafka;

public interface IKafkaConsumer : IDisposable
{
	Action<IConsumer<string, GenericRecord>, LogMessage> OnLogHandler { get; set; }
	Action<IConsumer<string, GenericRecord>, Error> OnErrorHandler { get; set; }
	IKafkaConsumer Create(Action<ConsumerConfig> action);
	Task Subscribe(string regexTopic, Func<ConsumeResult<string, GenericRecord>, Task> func, CancellationToken cancellationToken);
	Task Subscribe(IEnumerable<string> topics, Func<ConsumeResult<string, GenericRecord>, Task> func, CancellationToken cancellationToken);
}

internal class KafkaConsumer : IKafkaConsumer
{
	private IConsumer<string, GenericRecord> Consumer { get; set; }

	private readonly KafkaOptions _kafkaOptions;

	private readonly ISchemaRegistryClient _schemaRegistryClient;

	public KafkaConsumer(KafkaOptions kafkaOptions, ISchemaRegistryClient schemaRegistryClient) 
	{
		_kafkaOptions = kafkaOptions;
		_schemaRegistryClient = schemaRegistryClient;
	}

	public Action<IConsumer<string, GenericRecord>, LogMessage> OnLogHandler { get; set; }

	public Action<IConsumer<string, GenericRecord>, Error> OnErrorHandler { get; set; }

	private async Task Consume(Func<ConsumeResult<string, GenericRecord>, Task> func, CancellationToken cancellationToken)
	{
		try 
		{
			while (!cancellationToken.IsCancellationRequested)
			{
				var consumeResult = Consumer.Consume(cancellationToken);

				if (consumeResult != null)
				{
					await func(consumeResult);
				}

				Consumer.Commit(consumeResult);
			}
		}
		catch (OperationCanceledException)
		{
			this.Dispose();
		}
	}

	public void Dispose() => Consumer?.Close();

	public IKafkaConsumer Create(Action<ConsumerConfig> action)
	{
		var consumerConfig = _kafkaOptions.ConsumerConfig;

		consumerConfig.EnableAutoCommit = false;

		action(consumerConfig);

		var avroDeserializerConfig = new AvroDeserializerConfig();

		var consumerBuilder = new ConsumerBuilder<string, GenericRecord>(consumerConfig);
		//consumerBuilder.SetValueDeserializer(KafkaMessageSerializer<KafkaMessage>.Create());
		consumerBuilder.SetValueDeserializer(new AvroDeserializer<GenericRecord>(_schemaRegistryClient, avroDeserializerConfig).AsSyncOverAsync());
		consumerBuilder.SetLogHandler(OnLogHandler);
		consumerBuilder.SetErrorHandler(OnErrorHandler);
		Consumer = consumerBuilder.Build();

		return this;
	}

	public Task Subscribe(string regexTopic, Func<ConsumeResult<string, GenericRecord>, Task> func, CancellationToken cancellationToken)
	{
		Consumer.Subscribe(regexTopic);

		return Consume(func, cancellationToken);
	}

	public Task Subscribe(IEnumerable<string> topics, Func<ConsumeResult<string, GenericRecord>, Task> func, CancellationToken cancellationToken)
	{
		Consumer.Subscribe(topics);
		return Consume(func, cancellationToken);
	}
}
