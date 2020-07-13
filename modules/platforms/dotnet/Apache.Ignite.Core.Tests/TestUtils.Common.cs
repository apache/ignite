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
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Test utility methods.
    /// </summary>
    public static partial class TestUtils
    {
        /** Indicates long running and/or memory/cpu intensive test. */
        public const string CategoryIntensive = "LONG_TEST";

        /** Indicates examples tests. */
        public const string CategoryExamples = "EXAMPLES_TEST";

        /** */
        public const int DfltBusywaitSleepInterval = 200;

        /** System cache name. */
        public const string UtilityCacheName = "ignite-sys-cache";

        /** Work dir. */
        private static readonly string WorkDir =
            // ReSharper disable once AssignNullToNotNullAttribute
            Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "ignite_work");

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
                "-Xms64m",
                "-Xmx99m",
                "-ea",
                "-DIGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE=1000",
                "-DIGNITE_QUIET=true",
                "-Duser.timezone=UTC"
            };

        /** */
        private static readonly IList<string> JvmDebugOpts =
            new List<string> { "-Xdebug", "-Xnoagent", "-Djava.compiler=NONE", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005", "-DIGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP=false" };

        /** */
        public static bool JvmDebug = true;

        /** */
        [ThreadStatic]
        private static Random _random;

        /** */
        private static int _seed = Environment.TickCount;

        /// <summary>
        ///
        /// </summary>
        public static Random Random
        {
            get { return _random ?? (_random = new Random(Interlocked.Increment(ref _seed))); }
        }

        /// <summary>
        /// Gets current test name.
        /// </summary>
        public static string TestName
        {
            get { return TestContext.CurrentContext.Test.Name; }
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
        /// Waits for particular topology on specific cache (system cache by default).
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="waitingTop">Topology version.</param>
        /// <param name="cacheName">Cache name.</param>
        /// <param name="timeout">Timeout.</param>
        /// <returns>
        ///   <c>True</c> if topology took required size.
        /// </returns>
        public static bool WaitTopology(this IIgnite grid, AffinityTopologyVersion waitingTop,
            string cacheName = UtilityCacheName, int timeout = 30000)
        {
            int checkPeriod = 200;

            // Wait for late affinity.
            for (var iter = 0;; iter++)
            {
                var result = grid.GetCompute().ExecuteJavaTask<long[]>(
                    "org.apache.ignite.platform.PlatformCacheAffinityVersionTask", cacheName);
                var top = new AffinityTopologyVersion(result[0], (int) result[1]);
                if (top.CompareTo(waitingTop) >= 0)
                {
                    Console.Out.WriteLine("Current topology: " + top);
                    break;
                }

                if (iter % 10 == 0)
                    Console.Out.WriteLine("Waiting topology cur=" + top + " wait=" + waitingTop);

                if (iter * checkPeriod > timeout)
                    return false;

                Thread.Sleep(checkPeriod);
            }

            return true;
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
        /// Waits for condition, polling in a busy wait loop, then asserts that condition is true.
        /// </summary>
        /// <param name="cond">Condition.</param>
        /// <param name="timeout">Timeout, in milliseconds.</param>
        /// <param name="message">Assertion message.</param>
        public static void WaitForTrueCondition(Func<bool> cond, int timeout = 1000, string message = null)
        {
            var res = WaitForCondition(cond, timeout);
            message = message ?? string.Format("Condition not reached within {0} ms", timeout);

            Assert.IsTrue(res, message);
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
        /// Gets cache keys.
        /// </summary>
        public static IEnumerable<int> GetKeys(IIgnite ignite, string cacheName,
            IClusterNode node = null, bool primary = true)
        {
            var aff = ignite.GetAffinity(cacheName);
            node = node ?? ignite.GetCluster().GetLocalNode();

            return Enumerable.Range(1, int.MaxValue).Where(x => aff.IsPrimary(node, x) == primary);
        }

        /// <summary>
        /// Gets the primary keys.
        /// </summary>
        public static IEnumerable<int> GetPrimaryKeys(IIgnite ignite, string cacheName,
            IClusterNode node = null)
        {
            return GetKeys(ignite, cacheName, node);
        }

        /// <summary>
        /// Gets the primary key.
        /// </summary>
        public static int GetPrimaryKey(IIgnite ignite, string cacheName, IClusterNode node = null)
        {
            return GetPrimaryKeys(ignite, cacheName, node).First();
        }

        /// <summary>
        /// Gets the primary key.
        /// </summary>
        public static int GetKey(IIgnite ignite, string cacheName, IClusterNode node = null, bool primaryKey = false)
        {
            return GetKeys(ignite, cacheName, node, primaryKey).First();
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

            var items = handleRegistry.GetItems().Where(x => !(x.Value is LifecycleHandlerHolder)).ToList();

            if (items.Any())
            {
                Assert.Fail("HandleRegistry is not empty in grid '{0}' (expected {1}, actual {2}):\n '{3}'",
                    grid.Name, expectedCount, handleRegistry.Count,
                    items.Select(x => x.ToString()).Aggregate((x, y) => x + "\n" + y));
            }
        }

        /// <summary>
        /// Serializes and deserializes back an object.
        /// </summary>
        public static T SerializeDeserialize<T>(T obj, bool raw = false)
        {
            var cfg = new BinaryConfiguration
            {
                Serializer = raw ? new BinaryReflectiveSerializer {RawMode = true} : null
            };

            var marsh = new Marshaller(cfg) { CompactFooter = false };

            return marsh.Unmarshal<T>(marsh.Marshal(obj));
        }

        /// <summary>
        /// Clears the work dir.
        /// </summary>
        public static void ClearWorkDir()
        {
            if (!Directory.Exists(WorkDir))
            {
                return;
            }

            // Delete everything we can. Some files may be locked.
            foreach (var e in Directory.GetFileSystemEntries(WorkDir, "*", SearchOption.AllDirectories))
            {
                try
                {
                    File.Delete(e);
                }
                catch (Exception)
                {
                    // Ignore
                }

                try
                {
                    Directory.Delete(e, true);
                }
                catch (Exception)
                {
                    // Ignore
                }
            }
        }

        /// <summary>
        /// Gets the dot net source dir.
        /// </summary>
        public static DirectoryInfo GetDotNetSourceDir()
        {
            // ReSharper disable once AssignNullToNotNullAttribute
            var dir = new DirectoryInfo(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));

            while (dir != null)
            {
                if (dir.GetFiles().Any(x => x.Name == "Apache.Ignite.sln"))
                    return dir;

                dir = dir.Parent;
            }

            throw new InvalidOperationException("Could not resolve Ignite.NET source directory.");
        }

        /// <summary>
        /// Gets a value indicating whether specified partition is reserved.
        /// </summary>
        public static bool IsPartitionReserved(IIgnite ignite, string cacheName, int part)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(cacheName != null);

            const string taskName = "org.apache.ignite.platform.PlatformIsPartitionReservedTask";

            return ignite.GetCompute().ExecuteJavaTask<bool>(taskName, new object[] {cacheName, part});
        }

        /// <summary>
        /// Gets the innermost exception.
        /// </summary>
        public static Exception GetInnermostException(this Exception ex)
        {
            while (ex.InnerException != null)
            {
                ex = ex.InnerException;
            }

            return ex;
        }

        /// <summary>
        /// Gets the private field value.
        /// </summary>
        public static T GetPrivateField<T>(object obj, string name)
        {
            Assert.IsNotNull(obj);

            var field = obj.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);

            Assert.IsNotNull(field);

            return (T) field.GetValue(obj);
        }

        /// <summary>
        /// Gets active notification listeners.
        /// </summary>
        public static ICollection GetActiveNotificationListeners(this IIgniteClient client)
        {
            var failoverSocket = GetPrivateField<ClientFailoverSocket>(client, "_socket");
            var socket = GetPrivateField<ClientSocket>(failoverSocket, "_socket");
            return GetPrivateField<ICollection>(socket, "_notificationListeners");
        }
    }
}
