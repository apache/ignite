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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Test utility methods.
    /// </summary>
    public static class TestUtils
    {
        /** Indicates long running and/or memory/cpu intensive test. */
        public const string CategoryIntensive = "LONG_TEST";

        /** Indicates examples tests. */
        public const string CategoryExamples = "EXAMPLES_TEST";

        /** */
        public const int DfltBusywaitSleepInterval = 200;

        /** */

        private static readonly IList<string> TestJvmOpts = Environment.Is64BitProcess
            ? new List<string>
            {
                "-XX:+HeapDumpOnOutOfMemoryError",
                "-Xms1g",
                "-Xmx4g",
                "-ea",
                "-DIGNITE_QUIET=true",
                "-Duser.timezone=UTC"
            }
            : new List<string>
            {
                "-XX:+HeapDumpOnOutOfMemoryError",
                "-Xms512m",
                "-Xmx512m",
                "-ea",
                "-DIGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE=1000",
                "-DIGNITE_QUIET=true"
            };

        /** */
        private static readonly IList<string> JvmDebugOpts =
            new List<string> { "-Xdebug", "-Xnoagent", "-Djava.compiler=NONE", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" };

        /** */
        public static bool JvmDebug = true;

        /** */
        [ThreadStatic]
        private static Random _random;

        /** */
        private static int _seed = Environment.TickCount;

        /// <summary>
        /// Kill Ignite processes.
        /// </summary>
        public static void KillProcesses()
        {
            IgniteProcess.KillAll();
        }

        /// <summary>
        ///
        /// </summary>
        public static Random Random
        {
            get { return _random ?? (_random = new Random(Interlocked.Increment(ref _seed))); }
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public static IList<string> TestJavaOptions(bool? jvmDebug = null)
        {
            IList<string> ops = new List<string>(TestJvmOpts);

            if (jvmDebug ?? JvmDebug)
            {
                foreach (string opt in JvmDebugOpts)
                    ops.Add(opt);
            }

            return ops;
        }

        /// <summary>
        ///
        /// </summary>
        /// <returns></returns>
        public static string CreateTestClasspath()
        {
            return Classpath.CreateClasspath(forceTestClasspath: true);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="action"></param>
        /// <param name="threadNum"></param>
        public static void RunMultiThreaded(Action action, int threadNum)
        {
            List<Thread> threads = new List<Thread>(threadNum);

            var errors = new ConcurrentBag<Exception>();

            for (int i = 0; i < threadNum; i++)
            {
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        action();
                    }
                    catch (Exception e)
                    {
                        errors.Add(e);
                    }
                }));
            }

            foreach (Thread thread in threads)
                thread.Start();

            foreach (Thread thread in threads)
                thread.Join();
            
            foreach (var ex in errors)
                Assert.Fail("Unexpected exception: " + ex);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="action"></param>
        /// <param name="threadNum"></param>
        /// <param name="duration">Duration of test execution in seconds</param>
        public static void RunMultiThreaded(Action action, int threadNum, int duration)
        {
            List<Thread> threads = new List<Thread>(threadNum);

            var errors = new ConcurrentBag<Exception>();

            bool stop = false;

            for (int i = 0; i < threadNum; i++)
            {
                threads.Add(new Thread(() =>
                {
                    try
                    {
                        while (true)
                        {
                            Thread.MemoryBarrier();

                            // ReSharper disable once AccessToModifiedClosure
                            if (stop)
                                break;

                            action();
                        }
                    }
                    catch (Exception e)
                    {
                        errors.Add(e);
                    }
                }));
            }

            foreach (Thread thread in threads)
                thread.Start();

            Thread.Sleep(duration * 1000);

            stop = true;

            Thread.MemoryBarrier();

            foreach (Thread thread in threads)
                thread.Join();

            foreach (var ex in errors)
                Assert.Fail("Unexpected exception: " + ex);
        }

        /// <summary>
        /// Wait for particular topology size.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="size">Size.</param>
        /// <param name="timeout">Timeout.</param>
        /// <returns>
        ///   <c>True</c> if topology took required size.
        /// </returns>
        public static bool WaitTopology(this IIgnite grid, int size, int timeout = 30000)
        {
            int left = timeout;

            while (true)
            {
                if (grid.GetCluster().GetNodes().Count != size)
                {
                    if (left > 0)
                    {
                        Thread.Sleep(100);

                        left -= 100;
                    }
                    else
                        break;
                }
                else
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Asserts that the handle registry is empty.
        /// </summary>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        /// <param name="grids">Grids to check.</param>
        public static void AssertHandleRegistryIsEmpty(int timeout, params IIgnite[] grids)
        {
            foreach (var g in grids)
                AssertHandleRegistryHasItems(g, 0, timeout);
        }

        /// <summary>
        /// Asserts that the handle registry has specified number of entries.
        /// </summary>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        /// <param name="expectedCount">Expected item count.</param>
        /// <param name="grids">Grids to check.</param>
        public static void AssertHandleRegistryHasItems(int timeout, int expectedCount, params IIgnite[] grids)
        {
            foreach (var g in grids)
                AssertHandleRegistryHasItems(g, expectedCount, timeout);
        }

        /// <summary>
        /// Asserts that the handle registry has specified number of entries.
        /// </summary>
        /// <param name="grid">The grid to check.</param>
        /// <param name="expectedCount">Expected item count.</param>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        public static void AssertHandleRegistryHasItems(IIgnite grid, int expectedCount, int timeout)
        {
            var handleRegistry = ((Ignite)grid).HandleRegistry;

            expectedCount++;  // Skip default lifecycle bean

            if (WaitForCondition(() => handleRegistry.Count == expectedCount, timeout))
                return;

            var items = handleRegistry.GetItems().Where(x => !(x.Value is LifecycleBeanHolder)).ToList();

            if (items.Any())
                Assert.Fail("HandleRegistry is not empty in grid '{0}' (expected {1}, actual {2}):\n '{3}'", 
                    grid.Name, expectedCount, handleRegistry.Count,
                    items.Select(x => x.ToString()).Aggregate((x, y) => x + "\n" + y));
        }

        /// <summary>
        /// Waits for condition, polling in busy wait loop.
        /// </summary>
        /// <param name="cond">Condition.</param>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        /// <returns>True if condition predicate returned true within interval; false otherwise.</returns>
        public static bool WaitForCondition(Func<bool> cond, int timeout)
        {
            if (timeout <= 0)
                return cond();

            var maxTime = DateTime.Now.AddMilliseconds(timeout + DfltBusywaitSleepInterval);

            while (DateTime.Now < maxTime)
            {
                if (cond())
                    return true;

                Thread.Sleep(DfltBusywaitSleepInterval);
            }

            return false;
        }

        /// <summary>
        /// Gets the static discovery.
        /// </summary>
        public static TcpDiscoverySpi GetStaticDiscovery()
        {
            return new TcpDiscoverySpi
            {
                IpFinder = new TcpDiscoveryStaticIpFinder
                {
                    Endpoints = new[] { "127.0.0.1:47500" }
                },
                SocketTimeout = TimeSpan.FromSeconds(0.3)
            };
        }

        /// <summary>
        /// Gets the default code-based test configuration.
        /// </summary>
        public static IgniteConfiguration GetTestConfiguration(bool? jvmDebug = null)
        {
            return new IgniteConfiguration
            {
                DiscoverySpi = GetStaticDiscovery(),
                Localhost = "127.0.0.1",
                JvmOptions = TestJavaOptions(jvmDebug),
                JvmClasspath = CreateTestClasspath()
            };
        }

        /// <summary>
        /// Runs the test in new process.
        /// </summary>
        [SuppressMessage("ReSharper", "AssignNullToNotNullAttribute")]
        public static void RunTestInNewProcess(string fixtureName, string testName)
        {
            var procStart = new ProcessStartInfo
            {
                FileName = typeof(TestUtils).Assembly.Location,
                Arguments = fixtureName + " " + testName,
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            var proc = System.Diagnostics.Process.Start(procStart);

            Assert.IsNotNull(proc);

            Console.WriteLine(proc.StandardOutput.ReadToEnd());
            Console.WriteLine(proc.StandardError.ReadToEnd());
            Assert.IsTrue(proc.WaitForExit(15000));
            Assert.AreEqual(0, proc.ExitCode);
        }
    }
}
