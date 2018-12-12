
using System.Threading.Tasks;
using NUnit.Framework;

namespace NeCoreKafkaProducer.Tests
{
    public class KafkaProducerTest
    {
        [Test]
        public async Task ProduceTest()
        {
            KafkaTaskManager taskManager = new KafkaTaskManager();

            var success =  await KafkaTaskManager.Produce(new[] {"message"});
            
            Assert.IsTrue(success, "Should be able to produce on a kafka cluster");
        }
    }
}