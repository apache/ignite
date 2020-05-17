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
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl.Client.Compute;
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ComputeClient"/>.
    /// </summary>
    public class ComputeClientTests : ClientTestBase
    {
        /** */
        private const string TestResultCacheTask =
            "org.apache.ignite.internal.client.thin.ComputeTaskTest.TestResultCacheTask";
        
        /** */
        private const string TestFailoverTask =
            "org.apache.ignite.internal.client.thin.ComputeTaskTest.TestFailoverTask";
        
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
        public void TestExecuteJavaTaskWithKeepBinary()
        {
            // TODO: Binary mode on server and client.
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with no-failover flag.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithNoFailover()
        {
            // TODO:
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
            // TODO
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
            // TODO
        }

        /// <summary>
        /// Tests <see cref="IComputeClient.ExecuteJavaTask{TRes}"/> with custom cluster group.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskWithClusterGroup()
        {
            // TODO
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
        /// Tests <see cref="IComputeClient.ExecuteJavaTaskAsync{TRes}"/> from multiple threads.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncMultithreaded()
        {
            // TODO
        }

        /// <summary>
        /// Tests that <see cref="IComputeClient.ExecuteJavaTaskAsync{TRes}"/> with a long-running job
        /// does not block other client operations.
        /// </summary>
        [Test]
        public void TestExecuteJavaTaskAsyncWithLongJobDoesNotBlockOtherOperations()
        {
            // TODO
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
                        MaxActiveComputeTasksPerConnection = 10
                    }
                }
            };
        }
    }
}