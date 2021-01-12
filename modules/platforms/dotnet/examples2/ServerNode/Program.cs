using System;
using Apache.Ignite.Core;
using IgniteExamples.Shared;

namespace IgniteExamples.ServerNode
{
    public class Program
    {
        public static void Main()
        {
            using (Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.ReadKey();
            }
        }
    }
}
