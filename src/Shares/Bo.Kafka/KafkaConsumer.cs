using Bo.Kafka.Models;
using Confluent.Kafka;
using Bo.Kafka.Serializers;
using static Confluent.Kafka.ConfigPropertyNames;
using Confluent.Kafka.Admin;
using System.Text.RegularExpressions;

namespace Bo.Kafka;

public class KafkaConsumer : IDisposable
{
	private readonly IConsumer<Ignore, KafkaMessage> _consumer;

	public KafkaConsumer(ConsumerConfig config) 
	{
		var consumerBuilder = new ConsumerBuilder<Ignore, KafkaMessage>(config);
		consumerBuilder.SetValueDeserializer(KafkaMessageSerializer<KafkaMessage>.Create());
		consumerBuilder.SetLogHandler((_, logHandler) => { });
		consumerBuilder.SetErrorHandler((_, errorHandler) => { });
		_consumer = consumerBuilder.Build();
	}

	public async void Consume(string regexTopic, Func<KafkaMessage, Task> func, CancellationToken cancellationToken)
	{
		while (!cancellationToken.IsCancellationRequested)
		{
			_consumer.Subscribe("bo_connector_northwind_categories_topic");

			var consumeResult = _consumer.Consume(cancellationToken);

			if (consumeResult != null) 
			{
				await func(consumeResult.Message.Value);
			}
		}
	}

	public async void Consume(IEnumerable<string> topics, Func<KafkaMessage, Task> func, CancellationToken cancellationToken) 
	{
		while (!cancellationToken.IsCancellationRequested)
		{
			_consumer.Subscribe(topics);
			
			var consumeResult = _consumer.Consume(cancellationToken);

			if (consumeResult != null)
			{
				await func(consumeResult.Message.Value);
			}
		}
	}

	public async Task CreateTopic() 
	{
		var config = new AdminClientConfig { BootstrapServers = "localhost:29092" };

		using (var adminClient = new AdminClientBuilder(config).Build())
		{
			try
			{
				await adminClient.CreateTopicsAsync(new TopicSpecification[]
				{
					new TopicSpecification { Name = "bo_connector_northwind_categories_topic", NumPartitions = 1, ReplicationFactor = 1 }
				});
				Console.WriteLine("Topic created successfully.");
			}
			catch (CreateTopicsException e)
			{
				Console.WriteLine($"An error occurred creating topic: {e.Message}");
			}
		}
	}

	public void Dispose()
	{
		_consumer.Close();
	}
}
