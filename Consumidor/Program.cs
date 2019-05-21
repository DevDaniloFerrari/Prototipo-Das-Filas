using System;
using System.Threading.Tasks;

namespace OverviewRabbitMqDl
{
    class Program
    {
        static void Main(string[] args)
        {
            Consumidor consumidor = new Consumidor();
            consumidor.Iniciar();

            Console.ReadLine();
        }
    }
}
