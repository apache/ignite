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
    using System.Text;
    using Apache.Ignite.Benchmarks.Binary;

    /// <summary>
    /// Benchmark runner.
    /// </summary>
    internal class BenchmarkRunner
    {
        /// <summary>
        /// Entry point.
        /// </summary>
        /// <param name="args">Arguments.</param>
        // ReSharper disable once RedundantAssignment
        public static void Main(string[] args)
        {
            args = new[] { 
                typeof(BinarizableReadBenchmark).FullName,
                "-ConfigPath", @"modules\platforms\dotnet\Apache.Ignite.Benchmarks\Config\benchmark.xml",
                "-Threads", "1",
                "-Warmup", "0",
                "-Duration", "60",
                "-BatchSize", "1000"
            };

            var gcSrv = System.Runtime.GCSettings.IsServerGC;

            Console.WriteLine("GC Server: " + gcSrv);

            if (!gcSrv)
                Console.WriteLine("WARNING! GC server mode is disabled. This could yield in bad preformance.");

            Console.WriteLine("DotNet benchmark process started: " + Process.GetCurrentProcess().Id);

            var argsStr = new StringBuilder();

            foreach (var arg in args)
                argsStr.Append(arg + " ");
            
            if (args.Length < 1)
                throw new Exception("Not enough arguments: " + argsStr);
            
            Console.WriteLine("Arguments: " + argsStr);

            var benchmarkType = Type.GetType(args[0]);

            if (benchmarkType == null)
                throw new InvalidOperationException("Could not find benchmark type: " + args[0]);

            var benchmark = (BenchmarkBase)Activator.CreateInstance(benchmarkType);

            for (var i = 1; i < args.Length; i++)
            {
                var arg = args[i];

                if (arg.StartsWith("-"))
                    arg = arg.Substring(1);
                else
                    continue;

                var prop = BenchmarkUtils.GetProperty(benchmark, arg);

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
