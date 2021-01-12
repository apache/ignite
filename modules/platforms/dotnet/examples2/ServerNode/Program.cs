using System;
using Apache.Ignite.Core;

namespace ServerNode
{
    class Program
    {
        static void Main(string[] args)
        {
            // TODO: Reference shared assembly in this project.
            using (Ignition.Start())
            {
                Console.ReadKey();
            }
        }
    }
}
