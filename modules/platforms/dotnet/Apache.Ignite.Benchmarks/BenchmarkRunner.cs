/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Benchmarks
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using Apache.Ignite.Benchmarks.Interop;

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
                //typeof(GetAllBenchmark).FullName,
                typeof(GetAllBinaryBenchmark).FullName,
                //typeof(ThinClientGetAllBenchmark).FullName,
                //typeof(ThinClientGetAllBinaryBenchmark).FullName,
                "-ConfigPath", Directory.GetCurrentDirectory() + @"\..\..\Config\benchmark.xml",
                "-Threads", "1",
                "-Warmup", "0",
                "-Duration", "60",
                "-BatchSize", "1"
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
