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
    using System.Threading;
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
            // TODO:
        }

        /// <summary>
        /// Tests that <see cref="IIgniteClient.GetCompute"/> always returns the same instance.
        /// </summary>
        [Test]
        public void TestGetComputeAlwaysReturnsSameInstance()
        {
            Assert.AreSame(Client.GetCompute(), Client.GetCompute());
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