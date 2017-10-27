using Serilog;
using Serilog.Core;
using Serilog.Sinks.Elasticsearch;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ElasticMonitoring
{
    class Program
    {
        static Random Generator = new Random();

        static void Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Elasticsearch(new ElasticsearchSinkOptions(new Uri("http://localhost:9200"))
                {
                    CustomFormatter = new ExceptionAsObjectJsonFormatter(renderMessage: true)
                })
                .WriteTo.ColoredConsole()
                .CreateLogger();


            var generatedNumbers = new BufferBlock<int>();
            var actionBlock = new ActionBlock<int>(number =>
            {
                if (number % 10 == 0)
                {
                    logger
                    .ForContext("Failed", new
                    {
                        Count = 1,
                        Key = "failed",
                        Number = number
                    })
                    .Error(new InvalidOperationException("Number is divisible by 10"), "Failed");
                    return;
                }

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                var isPrime = IsPrime(number);

                stopwatch.Stop();

                logger.ForContext("Processed", new
                {
                    Key = "processed",
                    Number = number,
                    IsPrime = isPrime,
                    DurationInSeconds = stopwatch.Elapsed.TotalSeconds,
                    Count = 1
                },
                true).Information($"Processed, {number} in {stopwatch.Elapsed.TotalSeconds}s {isPrime}");
            }
            , new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 8
            });

            generatedNumbers.LinkTo(actionBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            Task.Run(async () =>
            {
                while (true)
                {
                    var count = Generator.Next(50);
                    for (var ix = 0; ix < count; ix++)
                    {
                        await generatedNumbers.SendAsync(Generator.Next(1000, 1000000));
                    }
                    logger.ForContext("Generated", new
                    {
                        Key = "generated",
                        Count = count
                    }, true)
                    .Information("Generated {@generatedCount}", count);
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            });

            Thread.CurrentThread.Join();
        }

        static bool IsPrime(int number)
        {
            Thread.Sleep(TimeSpan.FromSeconds(Generator.Next(10)));
            return !Enumerable.Range(2, number / 2 + 1).Any(div => number % div == 0);
        }
    }
}
