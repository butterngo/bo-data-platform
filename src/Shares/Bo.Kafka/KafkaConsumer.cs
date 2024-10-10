using Bo.Kafka.Models;
using Confluent.Kafka;
using Bo.Kafka.Serializers;

namespace Bo.Kafka;

public class KafkaConsumer : IDisposable
{
	private readonly IConsumer<Ignore, KafkaMessage> _consumer;

	public KafkaConsumer(ConsumerConfig config) 
	{
		config.EnableAutoCommit = false;
		var consumerBuilder = new ConsumerBuilder<Ignore, KafkaMessage>(config);
		consumerBuilder.SetValueDeserializer(KafkaMessageSerializer<KafkaMessage>.Create());
		consumerBuilder.SetLogHandler((_, logHandler) => { });
		consumerBuilder.SetErrorHandler((_, errorHandler) => { });
		_consumer = consumerBuilder.Build();
	}

	public async Task Consume(string regexTopic, Func<KafkaMessage, Task> func, CancellationToken cancellationToken)
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

	public async Task Consume(IEnumerable<string> topics, Func<KafkaMessage, Task> func, CancellationToken cancellationToken) 
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
	}
}
