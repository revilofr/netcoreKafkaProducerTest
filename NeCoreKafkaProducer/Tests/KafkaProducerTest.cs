
using System;
using System.Threading.Tasks;
using NeCoreKafkaProducer.Dto;
using NUnit.Framework;

namespace NeCoreKafkaProducer.Tests
{
    public class KafkaProducerTest
    {
        [Test]
        public async Task ProduceTest()
        {
            KafkaTaskManager taskManager = new KafkaTaskManager();
            KafkaConnection kConnection = new KafkaConnection
            {
                Url = "localhost",
                Port = 9092,
                Topic = "witsmlCreateEvent"
            };

            int timeout = 2000; 
            string message = $"message {new Random().Next()}";
            
            var success =  await KafkaTaskManager.Produce(kConnection,timeout,message);
            
            Assert.IsTrue(success, "Should be able to produce on a kafka cluster");
        }
    }
}