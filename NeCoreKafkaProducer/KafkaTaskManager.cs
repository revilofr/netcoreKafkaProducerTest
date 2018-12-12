
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace NeCoreKafkaProducer
{
    public class KafkaTaskManager
    {
        
        /// <summary>
        /// This task allows caller to produce a message
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static async Task<bool> Produce(string[] args)
        {
            //Todo pass the url as an argument
            //Check args
            String kafkaBrokerUrl = "localhost";
            String kafkaBrokerPort = "9092";
            int timeout = 1000;

            foreach (string arg in args)
            {
                
            }
            
            var config = new ProducerConfig { BootstrapServers = "10.0.2.15:9092" };

            // A Producer for sending messages with null keys and UTF-8 encoded values.
            using (var p = new Producer<Null, string>(config))
            {
                try
                {
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
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    return false;
                }
            }
        }

        /// <summary>
        /// This task allows caller to consume messages
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="groupId"></param>
        /// <param name="autoOffsetReset"></param>
        /// <param name="timeout">time in milisecond</param>
        /// <param name="action"></param>
        public static void Consume(string topic, string groupId, string autoOffsetReset, CancellationToken ct, Action<string> action)
        {
            long targetTime = long.MaxValue;
            
//            if (timeout != null)
//            {
//                targetTime = DateTime.Now.Ticks + (long)timeout*100000;
//            }
            
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumerConfig",
                BootstrapServers = "10.0.2.15:9092",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // eariest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            CheckConsumeParameters(topic, groupId, autoOffsetReset,  conf);


            using (var c = new Consumer<Ignore, string>(conf))
            {
                c.Subscribe(topic);

                bool consuming = true;
                // The client will automatically recover from non-fatal errors. You typically
                // don't need to take any action unless an error is marked as fatal.
                c.OnError += (_, e) => consuming = !e.IsFatal;

                //Todo handle time out
                //while (targetTime > DateTime.Now.Ticks && consuming )
                while (consuming )
                {
                    try
                    {
                        var cr = c.Consume();
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        
                        //Callback each time a message is consumed
                        action(cr.Value);

                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
            }

        }

        
        /// <summary>
        /// This method aims to check needed parameters for a basic kafka consumer
        /// </summary>
        /// <param name="topic">the name of the topic</param>
        /// <param name="groupId">the consumer group id</param>
        /// <param name="autoOffsetReset">utoOffsetReset mode</param>
        /// <param name="conf">Consumer configuration</param>
        /// <exception cref="ArgumentException"></exception>
        private static void CheckConsumeParameters(string topic, string groupId, string autoOffsetReset, ConsumerConfig conf)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("topic cannot be null or empty");
            }

            if (string.IsNullOrEmpty(groupId))
            {
                throw new ArgumentException("groupId cannot be null or empty");
            }
            if (!string.IsNullOrEmpty(autoOffsetReset))
            {
                autoOffsetReset = autoOffsetReset.ToLower().Trim();
                switch (autoOffsetReset)
                {
                    case "earliest":
                        conf.AutoOffsetReset = AutoOffsetResetType.Earliest;
                        break;
                    case "error":
                        conf.AutoOffsetReset = AutoOffsetResetType.Error;
                        break;
                    case "latest":
                        conf.AutoOffsetReset = AutoOffsetResetType.Latest;
                        break;
                }
            }

            if (!string.IsNullOrEmpty(groupId))
            {
                conf.GroupId = groupId;
            }
        }
    }
}