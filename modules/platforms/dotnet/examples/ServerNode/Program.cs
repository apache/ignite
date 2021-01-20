namespace IgniteExamples.ServerNode
{
    using System;
    using Apache.Ignite.Core;
    using IgniteExamples.Shared;

    public static class Program
    {
        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Utils.DeployDefaultServices(ignite);

                Console.WriteLine();
                Console.WriteLine(">>> Server node started, press any key to exit ...");

                Console.ReadKey();
            }
        }
    }
}
