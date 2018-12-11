
using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace NeCoreKafkaProducer
{
    public class KafkaTaskManager
    {
        public static async Task<bool> Produce(string[] args)
        {
            //Todo pass the url as an argument
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            // A Producer for sending messages with null keys and UTF-8 encoded values.
            using (var p = new Producer<Null, string>(config))
            {
                try
                {
                    int timeout = 1000;
                    var dr = p.ProduceAsync("witsmlCreateEvent", new Message<Null, string> { Value="test" });
                    if (await Task.WhenAny(dr, Task.Delay(timeout)) == dr)
                    {
                        var taskResult = await dr;
                        Console.WriteLine($"Delivered '{taskResult.Value}' to '{taskResult.TopicPartitionOffset}'");
                        return true;
                        // task completed within timeout
                    } else { 
                        // timeout logic
                        Console.WriteLine($"Request timedout");
                        return false;
                    }
                    
                    
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