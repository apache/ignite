/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.NuGet
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using NUnit.ConsoleRunner;

    /// <summary>
    /// Console test runner
    /// </summary>
    public static class TestRunner
    {
        [STAThread]
        static void Main()
        {
            Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            Debug.AutoFlush = true;

            TestAllInAssembly();
        }

        private static void TestOne(Type testClass, string method)
        {
            string[] args = { "/run:" + testClass.FullName + "." + method, Assembly.GetAssembly(testClass).Location };

            int returnCode = Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

        private static void TestAll(Type testClass)
        {
            string[] args = { "/run:" + testClass.FullName, Assembly.GetAssembly(testClass).Location };

            int returnCode = Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

        private static void TestAllInAssembly()
        {
            string[] args = { Assembly.GetAssembly(typeof(CacheTest)).Location };

            int returnCode = Runner.Main(args);

            if (returnCode != 0)
                Console.Beep();
        }

    }
}
