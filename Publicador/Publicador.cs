using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using RabbitMQ.Client;

namespace OverviewRabbitMqDl
{
    internal class Publicador
    {
        private readonly IConnectionFactory factory;

        private const string EXCHANGE = "nome-exchange";
        
        internal Publicador()
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
                IDictionary<string, object> args = new Dictionary<string, object>();
                channel.ExchangeDeclare(EXCHANGE, ExchangeType.Topic, true, false, args);
            }
        }

        internal void PublicarAlgumasMensagens(int quantasMensagens)
        {
            using (var conn = factory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                channel.ConfirmSelect();
                
                IBasicProperties props = channel.CreateBasicProperties();

                props.ContentType = "text/plain";
                props.DeliveryMode = 2;
                props.Persistent = true;
                
                for (int i = 1; i <= quantasMensagens; i++)
                {
                    var body = Encoding.UTF8.GetBytes($"Mensagem número {i}");
                    channel.BasicPublish(EXCHANGE, "teste", props, body);
                    if (!channel.WaitForConfirms())
                        throw new Exception("A mensagem não pôde ser publicada");

                    Console.WriteLine($"Mensagem {i} publicada.");
                }
            }
        }

        internal void PublicarMensagensSemParar(int intervaloEmSegundos)
        {
            using (var conn = factory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                channel.ConfirmSelect();
                
                IBasicProperties props = channel.CreateBasicProperties();

                props.ContentType = "text/plain";
                props.DeliveryMode = 2;
                props.Persistent = true;
                //props.Expiration = "500";

                while (true) 
                {
                    var body = Encoding.UTF8.GetBytes($"Mensagem xpto");
                    channel.BasicPublish(EXCHANGE, "teste", props, body);
                    if (!channel.WaitForConfirms())
                        throw new Exception("A mensagem não pôde ser publicada");

                    Console.WriteLine($"Mensagem publicada.");
                    
                    //Thread.Sleep(intervaloEmSegundos * 1000);
                }
                
            }
        }
    }
}
