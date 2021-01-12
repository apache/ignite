using System;
using Apache.Ignite.Core;

namespace IgniteExamples.ServerNode
{
    public class Program
    {
        public static void Main()
        {
            using (Ignition.Start())
            {
                Console.ReadKey();
            }
        }
    }
}
