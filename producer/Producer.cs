using Confluent.Kafka;
using System;

namespace producer
{
    /// <summary>
    /// Class in charge of sending data to apache kafka 
    /// </summary>
    class Producer
    {

        public void InitProducer()
        {
            while (true)
            {
                string message = Console.ReadLine();
                SendMessage(message);
            }
        }

        private ProducerConfig config()
        {
            return new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };
        }


        private async void SendMessage(string message)
        {

            using var producer = new ProducerBuilder<Null, string>(config()).Build();
            var taskProducer = await producer.ProduceAsync("topic-a", new Message<Null, string> { Value = message });

            if (taskProducer == null)
            {
                Console.WriteLine($"Error");
            }
            else
            {
                Console.WriteLine($"Wrote to offset: {taskProducer}");
            }

            producer.Flush(TimeSpan.FromSeconds(10));

        }
    }
}
