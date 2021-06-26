using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MQTest
{
    [TestClass]
    public class RabbitMQTest
    {
        private const string FileQueue = "Files";
        private const string ClientStatusExchange = "ClientStatus";
        private const string ServerStatusExchange = "ServerStatus";
        private const int MaxMessageSize = 1048576;
        private readonly byte[] StatusWaiting = Encoding.UTF8.GetBytes(Environment.UserName + " Waiting...");
        private readonly byte[] StatusUploading = Encoding.UTF8.GetBytes(Environment.UserName + " Uploading...");
        [TestMethod]
        public void SendFileTest()
        {
            try
            {
                const string filePath = @"F:/bandicam 2020-06-08 02-49-31-021.mp4";
                var factory = new ConnectionFactory()
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

                        var serverStatusQueue = channel.QueueDeclare().QueueName;
                        channel.QueueBind(serverStatusQueue, ServerStatusExchange, string.Empty);
                        var serverStatusConsumer = new EventingBasicConsumer(channel);
                        serverStatusConsumer.Received += ServerStatusConsumerOnReceived;
                        channel.BasicConsume(serverStatusQueue, true, serverStatusConsumer);
                        channel.BasicPublish(ClientStatusExchange, string.Empty, null, StatusUploading);

                        var messageId = Guid.NewGuid() + Path.GetFileName(filePath);
                        using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                        {
                            var count = 0;
                            for (var offset = 0; offset < stream.Length; offset += MaxMessageSize)
                            {
                                var body = new byte[MaxMessageSize];
                                var props = channel.CreateBasicProperties();
                                props.Headers = new Dictionary<string, object>
                                {
                                    {"Position", count}
                                };

                                if (offset + MaxMessageSize >= stream.Length)
                                {
                                    var lastBody = new byte[stream.Length - offset];
                                    props.Headers.Add("End", true);
                                    stream.Position = offset;
                                    stream.Read(lastBody, 0, (int) (stream.Length - offset));
                                    props.MessageId = messageId;
                                    channel.BasicPublish(string.Empty, FileQueue, props, lastBody);
                                    Console.WriteLine($@"Number: {count}");
                                    channel.BasicPublish(ClientStatusExchange, string.Empty, null, StatusWaiting);
                                    return;
                                }

                                stream.Position = offset;
                                stream.Read(body, 0, MaxMessageSize);
                                Console.WriteLine($@"Number: {count}");
                                props.MessageId = messageId;
                                channel.BasicPublish(string.Empty, FileQueue, props, body);
                                count++;
                                channel.BasicConsume(serverStatusQueue, true, serverStatusConsumer);
                            }
                        }
                        channel.BasicPublish(ClientStatusExchange, string.Empty, null, StatusWaiting);
                        channel.BasicConsume(serverStatusQueue, true, serverStatusConsumer);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private static void ServerStatusConsumerOnReceived(object sender, BasicDeliverEventArgs e)
        {
            Console.WriteLine($"{Encoding.UTF8.GetString(e.Body.ToArray())}");
        }
    }
}
