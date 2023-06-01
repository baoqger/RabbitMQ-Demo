using Microsoft.Extensions.Caching.Memory;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ.Services
{
    public class Consumer
    {
        private int _messageCount = 1;
        private readonly IMemoryCache _memoryCache;
        ConnectionFactory _factory { get; set; }
        IConnection _connection { get; set; }
        IModel _channel { get; set; }

        public Consumer(IMemoryCache memoryCache)
        {
            _memoryCache = memoryCache;
        }

        public void ReceiveMessageFromQ()
        {
            try
            {
                _factory = new ConnectionFactory() { HostName = "localhost" };
                _connection = _factory.CreateConnection();
                _channel = _connection.CreateModel();

                {
                    _channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);
                    var queueName = _channel.QueueDeclare(queue: "counter",
                                         durable: true,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    _channel.BasicQos(prefetchSize: 0, prefetchCount: 3, global: false);

                    _channel.QueueBind(queue: queueName,
                        exchange: "logs",
                        routingKey: "");

                    var consumer = new EventingBasicConsumer(_channel);

                    consumer.Received += async (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);
                        Thread.Sleep(3000);
                        _channel.BasicAck(ea.DeliveryTag, false);

                        Dictionary<string, int> messages = null;
                        _memoryCache.TryGetValue<Dictionary<string, int>>("messages", out messages);
                        if (messages == null) messages = new Dictionary<string, int>();
                        messages.Add(message, _messageCount);
                        _memoryCache.Set<Dictionary<string, int>>("messages", messages);
                    };

                    _channel.BasicConsume(queue: queueName,
                                         autoAck: false,
                                         consumer: consumer);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message} | {ex.StackTrace}");
            }
        }
    }
}
