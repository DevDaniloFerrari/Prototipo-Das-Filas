using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OverviewRabbitMqDl
{
    internal class Consumidor
    {
        private readonly IConnectionFactory factory;

        private const string EXCHANGE = "nome-exchange";
        private const string EXCHANGE_DL = "nome-exchange-dl";
        private const string FILA = "nome-fila";
        private const string FILA_RETRY = "nome-fila-retry";

        internal Consumidor()
        {
            factory = new ConnectionFactory
            {
                Uri = new Uri(@"amqp://guest:guest@172.20.7.29:5672")
            };

            Configurar();
        }

        private void Configurar()
        {
            using (var conn = factory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                IDictionary<string, object> exArgs = new Dictionary<string, object>();
                channel.ExchangeDeclare(EXCHANGE, ExchangeType.Topic, true, false, null);

                IDictionary<string, object> exDlArgs = new Dictionary<string, object>();
                channel.ExchangeDeclare(EXCHANGE_DL, ExchangeType.Topic, true, false, exDlArgs);

                IDictionary<string, object> filaDlArgs = new Dictionary<string, object>();
                filaDlArgs.Add("x-dead-letter-exchange", EXCHANGE);
                filaDlArgs.Add("x-message-ttl", 1000 * 10); // 10 segundos
                channel.QueueDeclare(FILA_RETRY, true, false, false, filaDlArgs);
                channel.QueueBind(FILA_RETRY, EXCHANGE_DL, "#", null);

                IDictionary<string, object> filaArgs = new Dictionary<string, object>();
                filaArgs.Add("x-dead-letter-exchange", EXCHANGE_DL);
                channel.QueueDeclare(FILA, true, false, false, filaArgs);
                channel.QueueBind(FILA, EXCHANGE, "#", filaArgs);
            }
        }

        internal void Iniciar()
        {
            using (var conn = factory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                channel.BasicQos(0, 1, false);

                var consumer = new EventingBasicConsumer(channel);

                var rand = new Random();

                consumer.Received += (ch, ea) =>
                {
                    try
                    {
                        var payload = Encoding.UTF8.GetString(ea.Body);

                        // Verifica se houve um "nack" previamente na mensagem.
                        if (ea.BasicProperties.Headers != null && ea.BasicProperties.Headers.ContainsKey("x-death"))
                        {
                            // O client .NET do rabbitmq é seu amigo, ele te dá tudo ou como object ou byte[]. Não gosta de string.
                            // Ou seja, vc tem que se foder pra achar as coisas por aqui.
                            var xdeathsRaw = (IList<object>)ea.BasicProperties.Headers["x-death"];

                            // Paranauê, Paranauê, Paraná...
                            var xdeaths = new List<dynamic>();
                        
                            foreach (var xdeathRaw in xdeathsRaw)
                            {
                                IDictionary<string, object> xdeathItemsRaw = (IDictionary<string, object>)xdeathRaw;
                                var reason = xdeathItemsRaw.Where(kv => kv.Key.ToLower() == "reason").First();
                                var count = xdeathItemsRaw.Where(kv => kv.Key.ToLower() == "count").First();
                                
                                var queue = xdeathItemsRaw.Where(kv => kv.Key.ToLower() == "queue").First();
                                var exchange = xdeathItemsRaw.Where(kv => kv.Key.ToLower() == "exchange").First();
                                
                                var routingKeysRaw = xdeathItemsRaw.Where(kv => kv.Key.ToLower() == "routing-keys").First();
                                var routingKeysItemsRaw = (IList<object>)routingKeysRaw.Value;
                                IList<string> routingKeys = routingKeysItemsRaw.Select(i => Encoding.UTF8.GetString((byte[])i) ).ToList();

                                var time = xdeathItemsRaw.Where(kv => kv.Key.ToLower() == "time").First();
                                
                                // Paranauê, Paranauê, Paraná...
                                xdeaths.Add(new 
                                {
                                    // Lembra da amizade do client? Então, tudo que é string, vem como byte[]...
                                    Reason = Encoding.UTF8.GetString((byte[])reason.Value),

                                    // mas quando é número, vem como object
                                    Count = Convert.ToInt32(count.Value),

                                    Queue = Encoding.UTF8.GetString((byte[])queue.Value),
                                    Exchange = Encoding.UTF8.GetString((byte[])exchange.Value),
                                    RoutingKeys = routingKeys,
                                    Time = time
                                });
                            }
                            
                            /*
                                Só nos interessa as mensagens rejeitadas pelo motivo "rejected"
                                que é quando rola um "nack"

                                Existe "rejected", "expired", "maxlength"
                                https://www.rabbitmq.com/dlx.html

                            */
                            var xdeathRejected = xdeaths.Where(
                                    xd => xd.Reason.ToLower() == "rejected"
                                ).FirstOrDefault();
                            
                            if (xdeathRejected != null)
                            {
                                Console.WriteLine($"\t{xdeathRejected.Count}x rejeitada(s)");
                            }
                        }

                        bool ok = rand.Next(1, 2 + 1) % 2 == 0;

                        if (ok && false)
                        {
                            channel.BasicAck(ea.DeliveryTag, false);
                        }
                        else
                        {
                            channel.BasicNack(ea.DeliveryTag, false, false);
                        }

                        Console.WriteLine($"Mensagem '{payload}' recebida. OK: {ok}");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"erro: {e}");
                    }
                };

                channel.BasicConsume(consumer, FILA, false);

                Console.WriteLine("Consumidor iniciado...");

                Console.ReadLine();
            }
        }
    }
}
