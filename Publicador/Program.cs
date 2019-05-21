using System;
using System.Threading.Tasks;

namespace OverviewRabbitMqDl
{
    class Program
    {
        static void Main(string[] args)
        {
            Publicador publicador = new Publicador();
            publicador.PublicarMensagensSemParar(1);
        }
    }
}
