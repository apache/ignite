/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using System.Text;
    using GridGain.Client.Benchmark.Portable;
    using BU = GridGain.Client.Benchmark.GridClientBenchmarkUtils;

    /// <summary>
    /// Benchmark runner.
    /// </summary>
    internal class GridClientBenchmarkRunner
    {
        /// <summary>
        /// Entry point.
        /// </summary>
        /// <param name="args">Arguments.</param>
        public static void Main(string[] args)
        {
            args = new string[] { 
                typeof(PortableWriteBenchmark).FullName,
                //"GridGain.Client.Benchmark.Interop.GridClientTaskBenchmark", 
                "-ConfigPath", "benchmarks/yardstick/config/single-node.xml",
                "-Threads", "1",
                "-Warmup", "0",
                "-Duration", "60",
                "-BatchSize", "1000"
            };

            bool gcSrv = System.Runtime.GCSettings.IsServerGC;

            Console.WriteLine("GC Server: " + gcSrv);

            if (!gcSrv)
                Console.WriteLine("WARNING! GC server mode is disabled. This could yield in bad preformance.");

            Console.WriteLine("DotNet benchmark process started: " + Process.GetCurrentProcess().Id);

            StringBuilder argsStr = new StringBuilder();

            foreach (string arg in args)
                argsStr.Append(arg + " ");
            
            if (args.Length < 1)
                throw new Exception("Not enough arguments: " + argsStr.ToString());
            else
                Console.WriteLine("Arguments: " + argsStr.ToString());

            Type benchmarkType = Type.GetType(args[0]);

            GridClientAbstractBenchmark benchmark = (GridClientAbstractBenchmark)Activator.CreateInstance(benchmarkType);

            for (int i = 1; i < args.Length; i++)
            {
                string arg = args[i];

                if (arg.StartsWith("-"))
                    arg = arg.Substring(1);
                else
                    continue;

                PropertyInfo prop = BU.FindProperty(benchmark, arg);

                if (prop != null)
                    benchmark.Configure(prop.Name, prop.PropertyType == typeof(bool) ? bool.TrueString : args[++i]);
            }

            benchmark.Run();

#if (DEBUG)
            Console.ReadLine();
#endif
        }
    }
}
