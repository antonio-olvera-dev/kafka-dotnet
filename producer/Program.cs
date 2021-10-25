using System;
using Confluent.Kafka;
namespace producer
{

    class Program
    {
        static void Main(string[] args)
        {


            while (true)
            {
                string message = Console.ReadLine();
                sendMessage(message);
            }


        }

        static ProducerConfig config()
        {
            return new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };
        }


        static void sendMessage(string message)
        {

            using (var producer = new ProducerBuilder<Null, string>(config()).Build())
            {
                var t = producer.ProduceAsync("topic-a", new Message<Null, string> { Value = message });
                t.ContinueWith(task =>
                {
                    if (task.IsFaulted)
                    {

                    }
                    else
                    {
                        Console.WriteLine($"Wrote to offset: {task.Result.Offset}");
                    }
                });

                producer.Flush(TimeSpan.FromSeconds(10));

            }

        }

    }

}