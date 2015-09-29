/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Benchmarks
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using System.Text;
    using Apache.Ignite.Benchmarks.Portable;
    using BU = Apache.Ignite.Benchmarks.GridClientBenchmarkUtils;

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
