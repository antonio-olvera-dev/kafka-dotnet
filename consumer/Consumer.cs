using Confluent.Kafka;
using System;
using System.Threading;

namespace consumer
{

    /// <summary>
    /// Class in charge of receiving data to apache kafka 
    /// </summary>
    class Consumer
    {

        private ConsumerConfig Config()
        {
            return new ConsumerConfig
            {
                GroupId = "grupo-a",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }


        public void InitConsumer()
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(Config()).Build();
            consumer.Subscribe("topic-a");

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancellationTokenSource.Cancel();
            };

            try
            {
                ReadMessage(consumer, cancellationTokenSource);
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }

        private static void ReadMessage(IConsumer<Ignore, string> consumer, CancellationTokenSource cancellationTokenSource)
        {
            while (true)
            {
                try
                {
                    var cr = consumer.Consume(cancellationTokenSource.Token);
                    Console.WriteLine(cr.Message.Value);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
        }
    }
}
