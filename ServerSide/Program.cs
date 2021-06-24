using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ServerSide
{
    internal static class Program
    {
        private const string FileQueue = "Files";
        private const string StatusQueue = "Status";
        private static readonly Dictionary<string, List<byte[]>> Files = new Dictionary<string, List<byte[]>>();

        private static void Main(string[] args)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest",
                Port = 5672,
                RequestedConnectionTimeout = TimeSpan.FromMilliseconds(3000)
            };
            try
            {
                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        channel.QueueDeclare(FileQueue, false, false, false, null);
                        var consumer = new EventingBasicConsumer(channel);
                        consumer.Received += ConsumerOnReceived;
                        while (true)
                        {
                            channel.BasicConsume(StatusQueue, true, consumer);
                            channel.BasicConsume(FileQueue, true, consumer);
                            Thread.Sleep(500);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static void ConsumerOnReceived(object sender, BasicDeliverEventArgs e)
        {
            if (!(sender is EventingBasicConsumer consumer))
            {
                return;
            }

            switch (e.RoutingKey)
            {
                case FileQueue:
                {
                    FileOnReceived(consumer, e);
                    break;
                }
                case StatusQueue:
                {
                    StatusOnReceived(e);
                    break;
                }
                default:
                    throw new ArgumentException($"Queue {e.RoutingKey} doesn't support");
            }
        }

        private static void FileOnReceived(EventingBasicConsumer consumer, BasicDeliverEventArgs e)
        {
            if (!(e.BasicProperties.Headers.TryGetValue("Position", out var pos)) || !int.TryParse(pos.ToString(), out var position))
            {
                return;
            }
            if (!Files.ContainsKey(e.BasicProperties.MessageId))
            {
                Files.Add(e.BasicProperties.MessageId, new List<byte[]>());
            }
            Files[e.BasicProperties.MessageId].Insert(position, e.Body.ToArray());
            if (e.BasicProperties.Headers.TryGetValue("End", out var end) && bool.TryParse(end.ToString(), out var res) && res)
            {
                var fileBody = new List<byte>();
                foreach (var filePart in Files[e.BasicProperties.MessageId])
                {
                    fileBody.AddRange(filePart);
                }
                File.WriteAllBytes($@"H:/{e.BasicProperties.MessageId}", fileBody.ToArray());
                Files[e.BasicProperties.MessageId].Clear();
                Files.Remove(e.BasicProperties.MessageId);
            }
            else
            {
                //consumer.Model.BasicConsume(FileQueue, true, consumer);
            }
        }

        private static void StatusOnReceived(BasicDeliverEventArgs e)
        {
            Console.WriteLine($"{Encoding.UTF8.GetString(e.Body.ToArray())}");
        }
    }
}
