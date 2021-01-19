namespace IgniteExamples.ServerNode
{
    using System;
    using Apache.Ignite.Core;
    using IgniteExamples.Shared;
    using IgniteExamples.Shared.Services;

    public static class Program
    {
        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                ignite.GetServices().DeployNodeSingleton("default-map-service", new MapService<int, string>());

                Console.WriteLine();
                Console.WriteLine(">>> Server node started, press any key to exit ...");

                Console.ReadKey();
            }
        }
    }
}
