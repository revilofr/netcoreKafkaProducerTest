using System.Runtime.CompilerServices;

namespace NeCoreKafkaProducer.Dto
{
    public class KafkaConnection
    {
        public string Url { get; set; }

        public int Port { get;  set;}

        public string Topic{ get;  set;}

    }
}