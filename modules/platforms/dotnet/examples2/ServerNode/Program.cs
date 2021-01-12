using System;
using System.Reflection;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using IgniteExamples.Shared;
using IgniteExamples.Shared.Models;

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
