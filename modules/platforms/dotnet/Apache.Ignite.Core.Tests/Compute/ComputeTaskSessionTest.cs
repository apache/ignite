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

namespace Apache.Ignite.Core.Tests.Compute
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;
    using static TestUtils;

    /// <summary>
    /// Tests for <see cref="IComputeTaskSession"/>
    /// </summary>
    public class ComputeTaskSessionTest
    {
        /// <summary>
        /// Data stored in a session by a task is available in the compute job created by the task on another node.
        /// </summary>
        [Test]
        public void DistributesTaskSessionAttributeRemotely()
        {
            // Given an Ignite cluster consisting of server and client nodes
            using var ignored = Ignition.Start(GetIgniteConfiguration("server1"));
            using var ignite = Ignition.Start(GetIgniteConfiguration("client1", true));
            
            // When the user executes a task setting a session attribute and creating a job getting the attribute
            const string attrName = "attr1";
            const int attrValue = 123;
            var task = new SessionAttributeSetterTask(attrName);
            var sessionValue = ignite.GetCompute().Execute(task, attrValue);
            
            // Then the task returns the same attribute value
            Assert.AreEqual(attrValue, sessionValue);
        }

        /// <summary>
        /// Data stored in session by a task is available in the compute job created by the task on the same node.
        /// </summary>
        [Test]
        public void DistributesTaskSessionAttributeLocally()
        {
            // Given a single node Ignite cluster
            using var ignite = Ignition.Start(GetIgniteConfiguration("server1"));
            
            // When the user executes a task setting a session attribute and creating a job getting the attribute
            const string attrName = "attr1";
            const int attrValue = 123;
            var task = new SessionAttributeSetterTask(attrName);
            var sessionValue = ignite.GetCompute().Execute(task, attrValue);
            
            // Then the task returns the same attribute value
            Assert.AreEqual(attrValue, sessionValue);
        }

        private static IgniteConfiguration GetIgniteConfiguration(string igniteName, bool isClient = false) =>
            new IgniteConfiguration
            {
                ClientMode = isClient,
                ConsistentId = igniteName,
                IgniteInstanceName = igniteName,
                DiscoverySpi = GetStaticDiscovery(),
                JvmOptions = TestJavaOptions()
            };

        /// <summary>
        /// Sets the specified session attribute and creates one <see cref="SessionAttributeGetterJob"/>.
        /// </summary>
        [ComputeTaskSessionFullSupport]
        private class SessionAttributeSetterTask : ComputeTaskSplitAdapter<int, int, int>
        {
            private readonly string _attrName;
#pragma warning disable 649
            [TaskSessionResource] private IComputeTaskSession _taskSession;
#pragma warning restore 649

            public SessionAttributeSetterTask(string attrName)
            {
                _attrName = attrName;
            }

            /// <inheritdoc />
            public override int Reduce(IList<IComputeJobResult<int>> results) => results.Select(res => res.Data).Sum();

            /// <inheritdoc />
            protected override ICollection<IComputeJob<int>> Split(int gridSize, int attrValue)
            {
                _taskSession.SetAttributes(new KeyValuePair<string, int>(_attrName, attrValue));
                return new List<IComputeJob<int>> {new SessionAttributeGetterJob(_attrName)};
            }
        }

        /// <summary>
        /// Returns the specified session attribute.
        /// </summary>
        private class SessionAttributeGetterJob : ComputeJobAdapter<int>
        {
            private readonly string _attrName;
#pragma warning disable 649
            [TaskSessionResource] private IComputeTaskSession _taskSession;
#pragma warning restore 649

            public SessionAttributeGetterJob(string attrName)
            {
                _attrName = attrName;
            }

            /// <inheritdoc />
            public override int Execute() => _taskSession.GetAttribute<string, int>(_attrName);
        }
    }
}
