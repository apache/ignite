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
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using Apache.Ignite.Core.Tests.Binary;
    using Apache.Ignite.Core.Tests.Cache.Affinity;
    using Apache.Ignite.Core.Tests.Cache.Query;
    using Apache.Ignite.Core.Tests.Memory;
    using NUnit.ConsoleRunner;

    public static class TestRunner
    {
        [STAThread]
        static void Main(string[] args)
        {
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Debug.AutoFlush = true;

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

            TestOne(typeof(ConsoleRedirectTest), "TestMultipleDomains");

            //TestAll(typeof (AffinityFunctionTest));
            //TestAllInAssembly();
        }

        private static int TestOne(Type testClass, string method)
        {
            string[] args =
            {
                "/noshadow",
                "/run:" + testClass.FullName + "." + method,
                Assembly.GetAssembly(testClass).Location
            };

            int returnCode = Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();

            return returnCode;
        }

        private static void TestAll(Type testClass)
        {
            string[] args =
            {
                "/noshadow",
                "/run:" + testClass.FullName, Assembly.GetAssembly(testClass).Location
            };

            int returnCode = Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

        private static void TestAllInAssembly()
        {
            string[] args =
            {
                "/noshadow",
                Assembly.GetAssembly(typeof(InteropMemoryTest)).Location
            };

            int returnCode = Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

    }
}
