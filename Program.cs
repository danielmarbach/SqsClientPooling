using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace SqsPooling
{
    class Program
    {
        public static ConcurrentDictionary<long, (DateTimeOffset sentAt, long batch)> sent = new ConcurrentDictionary<long, (DateTimeOffset, long)>();
        public static ConcurrentDictionary<long, (DateTimeOffset sentAt, long batch)> received = new ConcurrentDictionary<long, (DateTimeOffset, long)>();
        static long receiveCount;
        static long sentCount;

        static SemaphoreSlim maxConcurrencySemaphore = new SemaphoreSlim(200);

        static int numberOfMessages = 20000;
        static CountdownEvent countDownEvent = new CountdownEvent(numberOfMessages);

        public static ConcurrentBag<StatsEntry> stats = new ConcurrentBag<StatsEntry>();
        static string EndpointName = "Sqs.Pooling.Tests";
        static string QueueName = EndpointName.Replace(".", "-");

        public struct StatsEntry
        {
            public long Id;
            public DateTimeOffset ReceivedAt;
            public DateTimeOffset SentAt;
            public long Batch;

            public StatsEntry(long id, long batch, DateTimeOffset sentAt, DateTimeOffset receivedAt)
            {
                Id = id;
                SentAt = sentAt;
                ReceivedAt = receivedAt;
                Batch = batch;
            }
        }

        static async Task Main(string[] args)
        {
            Console.Title = EndpointName;

            Console.WriteLine("Purging queues");

            string queueUrl = null;

            var client = new AmazonSQSClient(new AmazonSQSConfig());

            try
            {
                queueUrl = await CreateQueue(client, QueueName);
            }
            catch (QueueNameExistsException)
            {
            }

            try
            {
                var inputQueue = await client.GetQueueUrlAsync(QueueName);
                await PurgeByReceiving(client, queueUrl);
            }
            catch (PurgeQueueInProgressException)
            {

            }
            catch (QueueDoesNotExistException)
            {
            }

            Console.WriteLine("Queues purged.");

            bool pooling = true;
            var stopWatch = Stopwatch.StartNew();
            var cts = new CancellationTokenSource();
            var receiveClients = new List<IAmazonSQS>();

            await Sending(client, queueUrl, pooling);

            var consumerTasks = new List<Task>();
            for (var i = 0; i < 10; i++)
            {
                consumerTasks.Add(ConsumeMessage(client, receiveClients, queueUrl, i, cts.Token, pooling));
            }

            var waitEvent = Task.Run(() =>
            {
                countDownEvent.Wait();
            });
            await waitEvent;

            stopWatch.Stop();

            Console.WriteLine(stopWatch.Elapsed);

            cts.Cancel();

            await Task.WhenAll(consumerTasks);

            while (maxConcurrencySemaphore.CurrentCount != 100)
            {
                await Task.Delay(50).ConfigureAwait(false);
            }

            cts?.Dispose();
            maxConcurrencySemaphore?.Dispose();
            foreach (var receiveClient in receiveClients)
            {
                receiveClient.Dispose();
            }

            Console.WriteLine("Press any key to exit.");
            GC.Collect();

            await WriteStats();

            Console.ReadKey();
        }

        static async Task Sending(IAmazonSQS sharedClient, string queueUrl, bool pooling)
        {
            var attempt = 0L;
            var batchSend = 0L;

            IAmazonSQS[] clientPool = null;
            if (pooling)
            {
                clientPool = new IAmazonSQS[] { new AmazonSQSClient(new AmazonSQSConfig()), new AmazonSQSClient(new AmazonSQSConfig()), new AmazonSQSClient(new AmazonSQSConfig()) };
            }

            try
            {
                for (var i = 0; i < numberOfMessages / 1000; i++)
                {
                    var sendTask = new List<Task>(1000);
                    batchSend++;
                    for (var j = 0; j < 1000; j++)
                    {
                        async Task SendMessage(long localAttempt, long batchNr)
                        {
                            var now = DateTimeOffset.UtcNow;

                            try
                            {
                                sent.AddOrUpdate(localAttempt, (now, batchNr), (_, __) => (now, batchNr));

                                var client = pooling ? clientPool[localAttempt % clientPool.Length] : sharedClient;
                                await client.SendMessageAsync(new SendMessageRequest(queueUrl, $"{localAttempt};{now.ToUnixTimeMilliseconds()};{batchNr}"), CancellationToken.None);

                                Interlocked.Increment(ref sentCount);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Sending error {ex.Message}. Aborting");
                                if (!countDownEvent.IsSet)
                                {
                                    countDownEvent.Signal();
                                }
                            }
                        }

                        sendTask.Add(SendMessage(attempt++, batchSend));
                    }

                    await Task.WhenAll(sendTask);
                }

            }
            catch (OperationCanceledException)
            {
                // ignore
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Sending error {ex.Message}. Aborting");
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("--- Sending ---");
                Console.WriteLine("Done sending...");
                Console.WriteLine("--- Sending ---");

                if (pooling)
                {
                    foreach (var client in clientPool)
                    {
                        client.Dispose();
                    }
                }
            }
        }

        static async Task ConsumeMessage(IAmazonSQS sharedClient, List<IAmazonSQS> createdClients, string queueUrl, int pumpNumber, CancellationToken token, bool pooling)
        {
            IAmazonSQS client = sharedClient;
            if (pooling)
            {
                client = new AmazonSQSClient(new AmazonSQSConfig());
                createdClients.Add(client);
            }
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var receiveResult = await client.ReceiveMessageAsync(new ReceiveMessageRequest
                    {
                        MaxNumberOfMessages = 10,
                        QueueUrl = queueUrl,
                        WaitTimeSeconds = 20,
                        AttributeNames = new List<string>
                        {
                            "SentTimestamp",
                            "ApproximateFirstReceiveTimestamp",
                            "ApproximateReceiveCount"
                        },
                    },
                        token).ConfigureAwait(false);

                    foreach (var message in receiveResult.Messages)
                    {
                        try
                        {
                            await maxConcurrencySemaphore.WaitAsync(token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            // shutting, semaphore doesn't need to be released because it was never acquired
                            return;
                        }
                        Consume(client, queueUrl, message);
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - cancelled");
                }
                catch (OverLimitException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - throttled");
                }
                catch (AmazonSQSException)
                {
                    Console.WriteLine($"{DateTime.UtcNow} ({pumpNumber}) - error");
                }
            }
        }

        static async Task PurgeByReceiving(IAmazonSQS sqsClient, string queueUrl)
        {
            while (true)
            {
                var receiveResult = await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    MaxNumberOfMessages = 10,
                    QueueUrl = queueUrl,
                    WaitTimeSeconds = 1,
                }).ConfigureAwait(false);

                if (receiveResult.Messages.Count == 0)
                {
                    return;
                }

                await Task.WhenAll(receiveResult.Messages.Select(m => sqsClient.DeleteMessageAsync(queueUrl, m.ReceiptHandle, CancellationToken.None)).ToArray());

            }
        }

        static async Task Consume(IAmazonSQS sqsClient, string queueUrl, Message message)
        {
            try
            {
                Interlocked.Increment(ref receiveCount);
                if (!countDownEvent.IsSet)
                {
                    countDownEvent.Signal();
                }

                var sentTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(message.Attributes["SentTimestamp"]));
                var firstReceiveTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(message.Attributes["ApproximateFirstReceiveTimestamp"]));

                if (Convert.ToInt32(message.Attributes["ApproximateReceiveCount"]) > 1)
                {
                    firstReceiveTimestamp = DateTimeOffset.UtcNow;
                }

                var elapsed = firstReceiveTimestamp - sentTimestamp;

                var content = message.Body.Split(';');
                var attempt = Convert.ToInt64(content[0]);
                var batch = Convert.ToInt64(content[2]);
                var sentAt = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(content[1]));

                received.AddOrUpdate(attempt, (sentAt, batch), (_, __) => (sentAt, batch));
                stats.Add(new StatsEntry(attempt, batch, sentAt, DateTime.UtcNow));

                await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle, CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                maxConcurrencySemaphore.Release();
            }
        }

        static async Task<string> CreateQueue(AmazonSQSClient client, string queueName)
        {
            var sqsRequest = new CreateQueueRequest
            {
                QueueName = queueName
            };
            var createQueueResponse = await client.CreateQueueAsync(sqsRequest).ConfigureAwait(false);
            var queueUrl = createQueueResponse.QueueUrl;
            var sqsAttributesRequest = new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl
            };
            sqsAttributesRequest.Attributes.Add(QueueAttributeName.MessageRetentionPeriod,
                TimeSpan.FromDays(4).TotalSeconds.ToString());

            await client.SetQueueAttributesAsync(sqsAttributesRequest).ConfigureAwait(false);
            return queueUrl;
        }

        static async Task WriteStats()
        {
            using (var writer = new StreamWriter("stats.csv", false))
            {
                await writer.WriteLineAsync($"{nameof(StatsEntry.Id)},{nameof(StatsEntry.Batch)},{nameof(StatsEntry.SentAt)},{nameof(StatsEntry.ReceivedAt)},Delta");
                foreach (var statsEntry in stats.OrderBy(s => s.SentAt))
                {
                    var delta = statsEntry.ReceivedAt - statsEntry.SentAt;
                    await writer.WriteLineAsync($"{statsEntry.Id},{statsEntry.Batch},{statsEntry.SentAt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)},{statsEntry.ReceivedAt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)},{delta.ToString(@"hh\:mm\:ss\.fff", CultureInfo.InvariantCulture)}");
                }

                await writer.FlushAsync();
                writer.Close();
            }
        }
    }
}
