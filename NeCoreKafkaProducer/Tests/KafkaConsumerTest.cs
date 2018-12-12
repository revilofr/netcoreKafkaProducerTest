
using System;
using System.IO;
using System.Reflection;
using System.Threading;
using log4net;
using log4net.Config;
using NeCoreKafkaProducer.Dto;
using NUnit.Framework;

namespace NeCoreKafkaProducer.Tests
{
    [TestFixture]
    public class KafkaConsumerTest
    {

        private static bool isMessageConsumed;
        
        //private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        [OneTimeSetUp]
        public void setup()
        {
            //todo fix logger
            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            string log4netPath = Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.FullName,"log4net.config");
            XmlConfigurator.Configure(logRepository, new FileInfo(log4netPath));
            log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
            log.Debug("debug message");
            log.Error("ERROR message");
            log.Info("info message");
        }
        
        [Test]
        public void ConsumeTest()
        {
            KafkaConnection kConnection = new KafkaConnection
            {
                Url = "localhost",
                Port = 9092,
                Topic = "witsmlCreateEvent"
            };
            Action<string> onMessageConsumed= ConsumeMessage;
            KafkaTaskManager taskManager = new KafkaTaskManager();
            string group = $"test_{new Random().Next()}";
            string autoOffsetReset = "earliest";
            int timeOut = 10*1000;

            // Create the token source for cancelation purpose
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken ct = cts.Token;
            
            Assert.Throws<OperationCanceledException>(() =>
            {
                cts.CancelAfter(timeOut);
                KafkaTaskManager.Consume(kConnection, group, autoOffsetReset, ct, onMessageConsumed);
            });

            cts.Dispose();
            
            Assert.IsTrue(isMessageConsumed, "Message should be received from kafka");
        }

        private static void ConsumeMessage(string message)
        {
            isMessageConsumed = true;
            Console.WriteLine("Message consumed");
        }
    }
}