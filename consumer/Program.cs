using System;

namespace consumer
{

    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using Confluent.Kafka;

    class Program
    {

        static void Main(string[] args)
        {
            initConsumer();
        }

        static ConsumerConfig config()
        {
            return new ConsumerConfig
            {
                GroupId = "grupo-a",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }


        static void initConsumer()
        {
            using (var consumer = new ConsumerBuilder<Ignore, string>(config()).Build())
            {
                consumer.Subscribe("topic-a");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine(cr.Message.Value);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }

}
