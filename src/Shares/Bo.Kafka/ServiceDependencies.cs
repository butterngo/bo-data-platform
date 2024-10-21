using Confluent.SchemaRegistry;
using Microsoft.Extensions.DependencyInjection;

namespace Bo.Kafka;

public static class ServiceDependencies
{
	public static IServiceCollection AddKafka(this IServiceCollection services, Action<KafkaOptions> action)
	{
		KafkaOptions kafkaOptions = new();
		
		action(kafkaOptions);

		services.AddSingleton(p => kafkaOptions);

		services.AddSingleton<ISchemaRegistryClient>(p => new CachedSchemaRegistryClient(kafkaOptions.SchemaRegistryConfig));

		services.AddTransient<IKafkaProducer, KafkaProducer>();

		services.AddTransient<IKafkaConsumer, KafkaConsumer>();
		
		return services;
	}
}
