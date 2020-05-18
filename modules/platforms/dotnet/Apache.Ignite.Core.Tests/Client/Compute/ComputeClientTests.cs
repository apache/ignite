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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Compute;
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

            foreach (var entry in logger.Entries)
            {
                if (entry.Level >= LogLevel.Warn)
                {
                    Assert.Fail(entry.Message);
                }
            }
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
        public void TestExecuteJavaTaskWithError([Values(true, false)] bool async)
        {
            // TODO
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> throws exception
        /// when client disconnects during task execution.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskThrowsExceptionOnDisconnect([Values(true, false)] bool async)
        {
            // TODO
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
            // TODO: Why does this cause connection abort?
            var arg = new PlatformComputeNetBinarizable {Field = 42};

            var res = Client.GetCompute().ExecuteJavaTask<int>(ComputeApiTest.BinaryArgTask, arg);
            
            Assert.AreEqual(arg.Field, res);
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
            const long delayMs = 1000;
            
            var cts = new CancellationTokenSource();
            var task = Client.GetCompute().ExecuteJavaTaskAsync<object>(TestTask, delayMs, cts.Token);
            
            cts.Cancel();
            Assert.IsTrue(task.IsCanceled);
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
            // TODO: see testComputeWithMixedModificators in Java
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
            // TODO
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTaskAsync{TRes}(string,object)"/> from multiple threads.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncMultithreaded()
        {
            var count = 20000;
            var compute = Client.GetCompute().WithKeepBinary();
            
            TestUtils.RunMultiThreaded(() =>
            {
                while (true)
                {
                    var arg = new PlatformComputeNetBinarizable { Field = Interlocked.Decrement(ref count) };

                    var res = compute.ExecuteJavaTask<int>(ComputeApiTest.BinaryArgTask, arg);
                
                    Assert.AreEqual(arg.Field, res);

                    if (res < 0)
                    {
                        break;
                    }
                }
            }, MaxTasks - 1);
        }

        /// <summary>
        /// Tests that <see cref="IComputeClient.ExecuteJavaTaskAsync{TRes}(string,object)"/> with a long-running job
        /// does not block other client operations.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncWithLongJobDoesNotBlockOtherOperations()
        {
            // TODO
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
            
            Assert.AreEqual("Unknown pair [platformId=1, typeId=-422570294]", clientEx.Message);
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
    }
}