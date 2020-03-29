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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using Apache.Ignite.Core.Tests.Binary.Serializable;
    using Apache.Ignite.Core.Tests.Cache;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using Apache.Ignite.Core.Tests.Compute;
    using Apache.Ignite.Core.Tests.Memory;
    using NUnit.ConsoleRunner;

    /// <summary>
    /// Console test runner.
    /// </summary>
    public static class TestRunner
    {
        [STAThread]
        static void Main(string[] args)
        {
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Debug.AutoFlush = true;

            if (args.Length == 1 && args[0] == "-basicTests")
            {
                RunBasicTests();
                
                return;
            }

            if (args.Length == 2)
            {
                //Debugger.Launch();
                var testClass = Type.GetType(args[0]);
                var method = args[1];

                if (testClass == null || testClass.GetMethods().All(x => x.Name != method))
                    throw new InvalidOperationException("Failed to find method: " + testClass + "." + method);

                Environment.ExitCode = TestOne(testClass, method);
                return;
            }

            Environment.ExitCode = TestAllInAssembly();
        }

        /// <summary>
        /// Runs some basic tests.
        /// </summary>
        private static void RunBasicTests()
        {
            Console.WriteLine(">>> Starting basic tests...");

            var basicTests = new[]
            {
                typeof(ComputeApiTest),
                typeof(SqlDmlTest),
                typeof(LinqTest),
                typeof(PersistenceTest),
                typeof(CacheTest)
            };

            Environment.ExitCode = TestAll(basicTests, true);

            Console.WriteLine(">>> Test run finished.");
        }

        /// <summary>
        /// Runs specified test method.
        /// </summary>
        private static int TestOne(Type testClass, string method, bool sameDomain = false)
        {
            string[] args =
            {
                "-noshadow",
                "-domain:" + (sameDomain ? "None" : "Single"),
                "-run:" + testClass.FullName + "." + method,
                Assembly.GetAssembly(testClass).Location
            };

            return Runner.Main(args);
        }

        /// <summary>
        /// Runs all tests in specified class.
        /// </summary>
        private static int TestAll(IEnumerable<Type> testClass, bool sameDomain = false)
        {
            var args = new List<string>
            {
                "-noshadow",
                "-domain:" + (sameDomain ? "None" : "Single"),
                "-run:" + string.Join(",", testClass.Select(x => x.FullName)),
                Assembly.GetAssembly(typeof(TestRunner)).Location
            };
            
            return Runner.Main(args.ToArray());
        }

        /// <summary>
        /// Runs all tests in assembly.
        /// </summary>
        private static int TestAllInAssembly(bool sameDomain = false)
        {
            string[] args =
            {
                "-noshadow",
                "-domain:" + (sameDomain ? "None" : "Single"),
                Assembly.GetAssembly(typeof(InteropMemoryTest)).Location
            };

            return Runner.Main(args);
        }
    }
}