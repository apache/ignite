namespace IgniteExamples.ServerNode
{
    using System;
    using Apache.Ignite.Core;
    using IgniteExamples.Shared;

    public class Program
    {
        public static void Main()
        {
            using (Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Server node started, press any key to exit ...");

                Console.ReadKey();
            }
        }
    }
}
