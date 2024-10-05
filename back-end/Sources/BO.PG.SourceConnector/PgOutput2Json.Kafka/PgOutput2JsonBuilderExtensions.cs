//using System;

//namespace PgOutput2Json.Kafka
//{
//    public static class PgOutput2JsonBuilderExtensions
//    {
//        public static PgOutput2JsonBuilder UseRedis(this PgOutput2JsonBuilder builder,
//            Action<ConfigurationOptions>? configureAction = null)
//        {
//            var options = new ConfigurationOptions();

//            configureAction?.Invoke(options);

//            builder.WithMessagePublisherFactory(new RedisPublisherFactory(options));

//            return builder;
//        }
//    }
//}
