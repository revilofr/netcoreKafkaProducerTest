using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework.Internal;

namespace KafkaProducer
{
    class KafkaTaskManager
    {
        public static async Task<bool> Produce(string[] args)
        {
            //Todo pass the url as an argument
            var config = new ProducerConfig { BootstrapServers = "127.0.0.1:9092" };

            // A Producer for sending messages with null keys and UTF-8 encoded values.
            using (var p = new Producer<Null, string>(config))
            {
                try
                {
                    var dr = await p.ProduceAsync("witsmlCreateEvent", new Message<Null, string> { Value="test" });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    return true;
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    return false;
                }
            }
        }
    }
}