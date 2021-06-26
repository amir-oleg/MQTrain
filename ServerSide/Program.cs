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
        private const string ClientStatusExchange = "ClientStatus";
        private const string ServerStatusExchange = "ServerStatus";
        private static readonly byte[] StatusWaiting = Encoding.UTF8.GetBytes("Server waiting...");
        private static readonly byte[] StatusWorking = Encoding.UTF8.GetBytes("Server working...");
        private static readonly Dictionary<string, List<byte[]>> Files = new Dictionary<string, List<byte[]>>();

        private static void Main()
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest",
                    Port = 5672,
                    RequestedConnectionTimeout = TimeSpan.FromMilliseconds(3000)
                };
                using (var connection = factory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        channel.ExchangeDeclare(ClientStatusExchange, ExchangeType.Fanout);
                        channel.ExchangeDeclare(ServerStatusExchange, ExchangeType.Fanout);
                        channel.QueueDeclare(FileQueue, false, false, false, null);
                        var statusQueue = channel.QueueDeclare().QueueName;
                        channel.QueueBind(statusQueue, ClientStatusExchange, string.Empty);
                        var consumer = new EventingBasicConsumer(channel);

                        consumer.Received += ConsumerOnReceived;

                        while (true)
                        {
                            channel.BasicConsume(statusQueue, true, consumer);
                            channel.BasicConsume(FileQueue, true, consumer);
                            channel.BasicPublish(ServerStatusExchange, string.Empty, null, StatusWaiting);
                            //Thread.Sleep(500);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.Read();
            }
        }

        private static void ConsumerOnReceived(object sender, BasicDeliverEventArgs e)
        {
            if (!(sender is EventingBasicConsumer consumer))
            {
                return;
            }
            
            if (e.RoutingKey == FileQueue)
            {
                FileOnReceived(consumer, e);
            }
            else if (e.Exchange == ClientStatusExchange)
            {
                StatusOnReceived(e);
            }
        }

        private static void FileOnReceived(EventingBasicConsumer consumer, BasicDeliverEventArgs e)
        {
            if (!(e.BasicProperties.Headers.TryGetValue("Position", out var pos)) || !int.TryParse(pos.ToString(), out var position))
            {
                return;
            }

            consumer.Model.BasicPublish(ServerStatusExchange, string.Empty, null, StatusWorking);

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
        }

        private static void StatusOnReceived(BasicDeliverEventArgs e)
        {
            Console.WriteLine($"{Encoding.UTF8.GetString(e.Body.ToArray())}");
        }
    }
}
