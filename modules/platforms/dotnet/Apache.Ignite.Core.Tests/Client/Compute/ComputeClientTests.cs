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

namespace Apache.Ignite.Core.Tests.Client.Compute
{
    using System;
    using System.Collections;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Client.Compute;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ComputeClient"/>.
    /// </summary>
    public class ComputeClientTests : ClientTestBase
    {
        /** */
        private const string TestTask = "org.apache.ignite.internal.client.thin.TestTask";

        /** */
        private const string TestResultCacheTask = "org.apache.ignite.internal.client.thin.TestResultCacheTask";

        /** */
        private const string TestFailoverTask = "org.apache.ignite.internal.client.thin.TestFailoverTask";

        /** */
        private const string ActiveTaskFuturesTask = "org.apache.ignite.platform.PlatformComputeActiveTaskFuturesTask";

        /** */
        private const int MaxTasks = 8;

        /// <summary>
        /// Initializes a new instance of <see cref="ComputeClientTests"/>.
        /// </summary>
        public ComputeClientTests() : base(3)
        {
            // No-op.
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            var logger = (ListLogger) Client.GetConfiguration().Logger;
            var entries = logger.Entries;

            logger.Clear();

            foreach (var entry in entries)
            {
                if (entry.Level >= LogLevel.Warn)
                {
                    Assert.Fail(entry.Message);
                }
            }

            Assert.IsEmpty(Client.GetActiveNotificationListeners());
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/>.
        /// </summary>
        [Test]
        public void TestExecuteJavaTask([Values(true, false)] bool async)
        {
            var res = async
                ? Client.GetCompute().ExecuteJavaTask<int>(ComputeApiTest.EchoTask, ComputeApiTest.EchoTypeInt)
                : Client.GetCompute().ExecuteJavaTaskAsync<int>(ComputeApiTest.EchoTask, ComputeApiTest.EchoTypeInt)
                    .Result;

            Assert.AreEqual(1, res);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with server-side error.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithError()
        {
            var ex = (IgniteClientException) Assert.Throws<AggregateException>(
                    () => Client.GetCompute().ExecuteJavaTask<int>(ComputeApiTest.EchoTask, -42))
                .GetInnermostException();

            StringAssert.EndsWith("Unknown type: -42", ex.Message);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> throws exception
        /// when client disconnects during task execution.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskThrowsExceptionOnDisconnect()
        {
            var cfg = new IgniteConfiguration(GetIgniteConfiguration())
            {
                AutoGenerateIgniteInstanceName = true
            };

            var ignite = Ignition.Start(cfg);

            var port = ignite.GetCluster().GetLocalNode().GetAttribute<int>("clientListenerPort");

            var clientCfg = new IgniteClientConfiguration(GetClientConfiguration())
            {
                Endpoints = new[] {"127.0.0.1:" + port}
            };

            var client = Ignition.StartClient(clientCfg);

            try
            {
                var task = client.GetCompute().ExecuteJavaTaskAsync<object>(TestTask, (long) 10000);
                ignite.Dispose();

                var ex = Assert.Throws<AggregateException>(() => task.Wait()).GetInnermostException();
                StringAssert.StartsWith("Task cancelled due to stopping of the grid", ex.Message);
            }
            finally
            {
                ignite.Dispose();
                client.Dispose();
            }
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with binary flag.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithKeepBinaryJavaOnlyResultClass()
        {
            var res = Client.GetCompute().WithKeepBinary().ExecuteJavaTask<IBinaryObject>(
                ComputeApiTest.EchoTask, ComputeApiTest.EchoTypeBinarizableJava);

            Assert.AreEqual(1, res.GetField<int>("field"));
            Assert.AreEqual("field", res.GetBinaryType().Fields.Single());
            Assert.AreEqual("org.apache.ignite.platform.PlatformComputeJavaBinarizable", res.GetBinaryType().TypeName);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with binary flag.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithKeepBinaryDotnetOnlyArgClass()
        {
            var arg = new PlatformComputeNetBinarizable {Field = 42};

            var res = Client.GetCompute().WithKeepBinary().ExecuteJavaTask<int>(ComputeApiTest.BinaryArgTask, arg);

            Assert.AreEqual(arg.Field, res);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with .NET-only arg class
        /// and without WithKeepBinary.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithDotnetOnlyArgClassThrowsCorrectException()
        {
            var arg = new PlatformComputeNetBinarizable {Field = 42};

            var ex = Assert.Throws<AggregateException>(() =>
                Client.GetCompute().ExecuteJavaTask<int>(ComputeApiTest.BinaryArgTask, arg));

            var clientEx = (IgniteClientException) ex.GetInnermostException();

            var expected = string.Format(
                "Failed to resolve .NET class '{0}' in Java [platformId=0, typeId=-315989221].",
                arg.GetType().FullName);

            Assert.AreEqual(expected, clientEx.Message);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with no-failover flag.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithNoFailover()
        {
            var computeWithFailover = Client.GetCompute();
            var computeWithNoFailover = Client.GetCompute().WithNoFailover();

            Assert.IsTrue(computeWithFailover.ExecuteJavaTask<bool>(TestFailoverTask, null));
            Assert.IsFalse(computeWithNoFailover.ExecuteJavaTask<bool>(TestFailoverTask, null));
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with no-result-cache flag.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithNoResultCache()
        {
            var computeWithCache = Client.GetCompute();
            var computeWithNoCache = Client.GetCompute().WithNoResultCache();

            Assert.IsTrue(computeWithCache.ExecuteJavaTask<bool>(TestResultCacheTask, null));
            Assert.IsFalse(computeWithNoCache.ExecuteJavaTask<bool>(TestResultCacheTask, null));
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with a user-defined timeout.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithTimeout()
        {
            const long timeoutMs = 50;
            var compute = Client.GetCompute().WithTimeout(TimeSpan.FromMilliseconds(timeoutMs));

            var ex = Assert.Throws<AggregateException>(() => compute.ExecuteJavaTask<object>(TestTask, timeoutMs * 4));
            var clientEx = (IgniteClientException) ex.GetInnermostException();

            StringAssert.StartsWith("Task timed out (check logs for error messages):", clientEx.Message);
        }

        /// <summary>
        /// Tests that <see cref="IIgniteClient.GetCompute"/> always returns the same instance.
        /// </summary>
        [Test]
        public void TestGetComputeAlwaysReturnsSameInstance()
        {
            Assert.AreSame(Client.GetCompute(), Client.GetCompute());
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with cancellation.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncCancellation()
        {
            const long delayMs = 10000;

            // GetActiveTaskFutures uses Task internally, so it always returns at least 1 future.
            var futs1 = GetActiveTaskFutures();
            Assert.AreEqual(1, futs1.Length);

            // Start a long-running task and verify that 2 futures are active.
            var cts = new CancellationTokenSource();
            var task = Client.GetCompute().ExecuteJavaTaskAsync<object>(TestTask, delayMs, cts.Token);

            var taskFutures = GetActiveTaskFutures();
            TestUtils.WaitForTrueCondition(() =>
            {
                taskFutures = GetActiveTaskFutures();
                return taskFutures.Length == 2;
            });

            // Cancel and assert that the future from the step above is no longer present.
            cts.Cancel();
            Assert.IsTrue(task.IsCanceled);

            TestUtils.WaitForTrueCondition(() =>
            {
                var futures = GetActiveTaskFutures();

                return futures.Length == 1 && !taskFutures.Contains(futures[0]);
            }, message: "Unexpected number of active tasks: " + GetActiveTaskFutures().Length);
        }

        /// <summary>
        /// Tests that cancellation does not affect finished tasks.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncCancelAfterFinish()
        {
            const long delayMs = 1;
            var cts = new CancellationTokenSource();
            var task = Client.GetCompute().ExecuteJavaTaskAsync<object>(TestTask, delayMs, cts.Token);

            task.Wait(cts.Token);
            cts.Cancel();

            Assert.IsTrue(task.IsCompleted);
            Assert.IsFalse(task.IsFaulted);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with custom cluster group.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithClusterGroup()
        {
            Func<IComputeClient, Guid[]> getProjection = c =>
            {
                var res = c.ExecuteJavaTask<IgniteBiTuple>(TestTask, null);

                return ((ArrayList) res.Item2).OfType<Guid>().ToArray();
            };

            // Default: full cluster.
            var nodeIds = Client.GetCluster().GetNodes().Select(n => n.Id).ToArray();

            CollectionAssert.AreEquivalent(nodeIds, getProjection(Client.GetCompute()));

            // One node.
            var nodeId = nodeIds[1];
            var proj = Client.GetCluster().ForPredicate(n => n.Id == nodeId);

            Assert.AreEqual(new[]{nodeId}, getProjection(proj.GetCompute()));

            // Two nodes.
            proj = Client.GetCluster().ForPredicate(n => n.Id != nodeId);

            CollectionAssert.AreEquivalent(new[] {nodeIds[0], nodeIds[2]}, getProjection(proj.GetCompute()));
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with custom cluster group.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithMixedModifiers()
        {
            const long timeoutMs = 200;

            var cluster = Client.GetCluster();
            var nodeId = cluster.GetNode().Id;

            var compute = cluster.ForPredicate(n => n.Id == nodeId)
                .GetCompute()
                .WithNoFailover()
                .WithKeepBinary()
                .WithTimeout(TimeSpan.FromMilliseconds(timeoutMs));

            // KeepBinary.
            var res = compute.ExecuteJavaTask<IBinaryObject>(
                ComputeApiTest.EchoTask, ComputeApiTest.EchoTypeBinarizableJava);

            Assert.AreEqual(1, res.GetField<int>("field"));

            // Timeout.
            var ex = Assert.Throws<AggregateException>(() => compute.ExecuteJavaTask<object>(TestTask, timeoutMs * 3));
            var clientEx = (IgniteClientException) ex.GetInnermostException();

            StringAssert.StartsWith("Task timed out (check logs for error messages):", clientEx.Message);

            // Flags.
            Assert.IsFalse(compute.ExecuteJavaTask<bool>(TestFailoverTask, null));
            Assert.IsFalse(compute.WithNoResultCache().ExecuteJavaTask<bool>(TestResultCacheTask, null));

            // Cluster.
            var executedNodeIds = (ArrayList) compute.ExecuteJavaTask<IgniteBiTuple>(TestTask, null).Item2;
            Assert.AreEqual(nodeId, executedNodeIds.Cast<Guid>().Single());
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with unknown task class.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithUnknownClass()
        {
            var ex = Assert.Throws<AggregateException>(
                    () => Client.GetCompute().ExecuteJavaTask<int>("bad", null));

            var innerEx = ex.GetInnermostException();

            StringAssert.StartsWith(
                "Unknown task name or failed to auto-deploy task (was task (re|un)deployed?) [taskName=bad, ",
                innerEx.Message);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with exceeded
        /// <see cref="ThinClientConfiguration.MaxActiveComputeTasksPerConnection"/>.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithExceededTaskLimit()
        {
            var compute = Client.GetCompute().WithKeepBinary();

            var tasks = Enumerable
                .Range(1, MaxTasks * 2)
                .Select(_ => (Task) compute.ExecuteJavaTaskAsync<object>(TestTask, (long) 500))
                .ToArray();

            var ex = Assert.Throws<AggregateException>(() => Task.WaitAll(tasks));
            var clientEx = (IgniteClientException) ex.GetInnermostException();

            StringAssert.StartsWith("Active compute tasks per connection limit (8) exceeded", clientEx.Message);
            Assert.AreEqual(ClientStatusCode.TooManyComputeTasks, clientEx.StatusCode);
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTaskAsync{TRes}(string,object)"/> from multiple threads.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncMultithreaded()
        {
            var count = 10000;
            var compute = Client.GetCompute().WithKeepBinary();
            var cache = Client.GetOrCreateCache<int, int>(TestUtils.TestName);
            cache[1] = 1;

            TestUtils.RunMultiThreaded(() =>
            {
                while (true)
                {
                    var arg = new PlatformComputeNetBinarizable { Field = Interlocked.Decrement(ref count) };

                    var res = compute.ExecuteJavaTask<int>(ComputeApiTest.BinaryArgTask, arg);

                    Assert.AreEqual(arg.Field, res);

                    // Perform other operations to mix notifications and responses.
                    Assert.AreEqual(1, cache[1]);
                    Assert.AreEqual(1, cache.GetAsync(1).Result);

                    if (res < 0)
                    {
                        break;
                    }
                }
            }, MaxTasks - 2);
        }

        /// <summary>
        /// Tests that <see cref="IComputeClient.ExecuteJavaTaskAsync{TRes}(string,object)"/> with a long-running job
        /// does not block other client operations.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncWithLongJobDoesNotBlockOtherOperations()
        {
            var task = Client.GetCompute().ExecuteJavaTaskAsync<object>(TestTask, (long) 500);

            Client.GetCacheNames();

            Assert.IsFalse(task.IsCompleted);

            task.Wait();
        }

        /// <summary>
        /// Tests that failed deserialization (caused by missing type) produces correct exception.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithFailedResultDeserializationProducesBinaryObjectException()
        {
            var ex = Assert.Throws<AggregateException>(
                () => Client.GetCompute().ExecuteJavaTask<object>(
                    ComputeApiTest.EchoTask, ComputeApiTest.EchoTypeBinarizableJava));

            var clientEx = (IgniteClientException) ex.GetInnermostException();

            Assert.AreEqual("Failed to resolve Java class 'org.apache.ignite.platform.PlatformComputeJavaBinarizable'" +
                            " in .NET [platformId=1, typeId=-422570294].", clientEx.Message);
        }

        /// <summary>
        /// Tests that client timeout can be shorter than task duration.
        /// </summary>
        [Test]
        public void TestClientTimeoutShorterThanTaskDuration()
        {
            const long timeoutMs = 300;

            var cfg = new IgniteClientConfiguration(GetClientConfiguration())
            {
                SocketTimeout = TimeSpan.FromMilliseconds(timeoutMs)
            };

            using (var client = Ignition.StartClient(cfg))
            {
                var compute = client.GetCompute().WithKeepBinary();

                var res = compute.ExecuteJavaTask<IgniteBiTuple>(TestTask, timeoutMs * 3);

                Assert.IsInstanceOf<Guid>(res.Item1);
            }
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    ThinClientConfiguration = new ThinClientConfiguration
                    {
                        MaxActiveComputeTasksPerConnection = MaxTasks
                    }
                }
            };
        }

        /** <inheritdoc /> */
        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration(base.GetClientConfiguration())
            {
                SocketTimeout = TimeSpan.FromSeconds(3),
                EnablePartitionAwareness = false
            };
        }

        /// <summary>
        /// Gets active task futures from all server nodes.
        /// </summary>
        private IgniteGuid[] GetActiveTaskFutures()
        {
            return Client.GetCompute().ExecuteJavaTask<IgniteGuid[]>(ActiveTaskFuturesTask, null);
        }
    }
}
