
using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace NeCoreKafkaProducer.Tests
{
    public class KafkaConsumerTest
    {

        private static bool isMessageConsumed;
        
        [Test]
        public async Task ConsumeTest()
        {
            Action<string> onMessageConsumed;
            KafkaTaskManager taskManager = new KafkaTaskManager();
            string topic = "witsmlCreateEvent";
            string group = "group1";
            string autoOffsetReset = "earliest";
            int timeOut = 2000;
            onMessageConsumed = ConsumeMessage;
            
            // Create the token source for cancelation purpose
            CancellationToken ct = new CancellationToken();
            
            KafkaTaskManager.Consume("witsmlCreateEvent", "group1", autoOffsetReset, ct, onMessageConsumed);
            
            Assert.IsTrue(isMessageConsumed, "Message should be received from kafka");
                
            
            
        }

        private static void ConsumeMessage(string message)
        {
            isMessageConsumed = true;
            Console.WriteLine();
        }
    }
}