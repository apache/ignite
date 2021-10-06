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
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Console test runner.
    /// </summary>
    public static class TestRunner
    {
        [STAThread]
        static void Main(string[] args)
        {
            System.Diagnostics.Debug.AutoFlush = true;

#if (!NETCOREAPP)
            System.Diagnostics.Debug.Listeners.Add(new System.Diagnostics.TextWriterTraceListener(Console.Out));
#endif

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

            // Default: test startup.
            new IgniteStartStopTest().TestStartDefault();
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

            throw new Exception("TODO" + args);
        }
    }
}
