//using Bo.Kafka;
//using Confluent.Kafka;

//namespace PgOutput2Json.Redis
//{
//	internal class KafkaPublisherFactory : IMessagePublisherFactory
//    {
//        private readonly KafkaProducer _producer;

//        public KafkaPublisherFactory(ProducerConfig config)
//        {
//			_producer = new KafkaProducer(config);
//        }

//        public IMessagePublisher CreateMessagePublisher(ILoggerFactory? loggerFactory)
//        {
//            return new RedisPublisher(_options, loggerFactory?.CreateLogger<RedisPublisher>());
//        }
//    }
//}
