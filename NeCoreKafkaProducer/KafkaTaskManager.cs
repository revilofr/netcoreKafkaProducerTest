
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NeCoreKafkaProducer.Dto;

namespace NeCoreKafkaProducer
{
    public class KafkaTaskManager
    {
        
        /// <summary>
        /// This task allows caller to produce a message
        /// </summary>
        /// <returns>Task<bool> true if task succeded</returns>
        public static async Task<bool> Produce(KafkaConnection kConnection, int timeout, string message)
        {
            var config = new ProducerConfig { BootstrapServers = $"{kConnection.Url}:{kConnection.Port}" };

            // A Producer for sending messages with null keys and UTF-8 encoded values.
            using (var p = new Producer<Null, string>(config))
            {
                try
                {
                    var dr = p.ProduceAsync(kConnection.Topic, new Message<Null, string> { Value=message });
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
        public static void Consume(KafkaConnection kConnection, string groupId, string autoOffsetReset, CancellationToken ct, Action<string> action)
        {                     
            var conf = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = $"{kConnection.Url}:{kConnection.Port}",
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // eariest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            CheckConsumeParameters(kConnection.Topic, groupId, autoOffsetReset,  conf);


            using (var c = new Consumer<Ignore, string>(conf))
            {
                c.Subscribe(kConnection.Topic);

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
                        var cr = c.Consume(ct);
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