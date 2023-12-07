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
    using System.Linq;
    using System.Reflection;
    using NUnit.Framework;

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
                TestOne(args[0], args[1]);

                return;
            }

            // Default: test startup.
            new IgniteStartStopTest().TestStartDefault();
        }

        /// <summary>
        /// Runs specified test method.
        /// </summary>
        private static void TestOne(string className, string methodName)
        {
            var fixtureClass = Type.GetType(className);

            if (fixtureClass == null)
            {
                throw new InvalidOperationException("Failed to find class: " + className);
            }

            var fixture = Activator.CreateInstance(fixtureClass);

            foreach (var setUpMethod in GetMethodsWithAttr<SetUpAttribute>(fixtureClass))
            {
                setUpMethod.Invoke(fixture, Array.Empty<object>());
            }

            var testMethod = fixtureClass.GetMethod(methodName);

            if (testMethod == null)
                throw new InvalidOperationException("Failed to find method: " + fixtureClass + "." + methodName);

            testMethod.Invoke(fixture, Array.Empty<object>());
        }

        private static IEnumerable<MethodInfo> GetMethodsWithAttr<T>(Type testClass)
        {
            return testClass.GetMethods().Where(m => m.GetCustomAttributes(true).Any(a => a is T));
        }
    }
}
