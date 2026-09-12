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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;
    using static TestUtils;

    /// <summary>
    /// Tests for <see cref="IComputeTaskContinuousMapper"/>
    /// </summary>
    public class ComputeTaskContinuousMapperTest
    {
        /// <inheritdoc cref="IComputeTaskContinuousMapper.Send{TRes}(Apache.Ignite.Core.Compute.IComputeJob{TRes})"/>
        [Test]
        public void DistributesJobsUsingLoadBalancer()
        {
            // Given an Ignite cluster consisting of 2 server and 1 client node
            using var server1 = Ignition.Start(GetIgniteConfiguration("server1"));
            using var server2 = Ignition.Start(GetIgniteConfiguration("server2"));
            using var ignite = Ignition.Start(GetIgniteConfiguration("client1", true));

            // When the user executes a task continuously submitting 3 jobs using the cluster's load balancer
            var result = ignite.GetCompute().Execute(new ContinuousTask(), 3);

            // Then some job was executed on server1 and another on server2
            Assert.True(result.Contains("server1"));
            Assert.True(result.Contains("server2"));
        }
        
        /// <inheritdoc cref="IComputeTaskContinuousMapper.Send{TRes}(Apache.Ignite.Core.Compute.IComputeJob{TRes},Apache.Ignite.Core.Cluster.IClusterNode)"/>
        [Test]
        public void SendsJobsToSpecificNode()
        {
            // Given an Ignite cluster consisting of 2 server and 1 client node
            using var server1 = Ignition.Start(GetIgniteConfiguration("server1"));
            using var server2 = Ignition.Start(GetIgniteConfiguration("server2"));
            using var ignite = Ignition.Start(GetIgniteConfiguration("client1", true));

            // When the user executes a task continuously submitting 3 jobs to the same server1
            var node1 = server1.GetCluster().GetLocalNode();
            var result = ignite.GetCompute().Execute(new ContinuousTask(node1), 3);

            // Then all the jobs were executed on server1 and never on server2
            Assert.True(result.Contains("server1"));
            Assert.False(result.Contains("server2"));
        }
        
        private class ContinuousTask : ComputeTaskAdapter<int, string, string>
        {
            [TaskContinuousMapperResource] private readonly IComputeTaskContinuousMapper _mapper = null!;
            private readonly IClusterNode _maybeTargetNode;
            private int _counter;

            /// <summary>
            /// The task maps all the jobs to the specified node if it is not <c>null</c>, otherwise the task maps
            /// jobs to nodes via Ignite load balancer.
            /// </summary>
            public ContinuousTask(IClusterNode maybeTargetNode = null)
            {
                _maybeTargetNode = maybeTargetNode;
            }
            
            /// <inheritdoc />
            public override IDictionary<IComputeJob<string>, IClusterNode> Map(IList<IClusterNode> subgrid, int arg)
            {
                _counter = arg;
                Send(new IgniteNameJob(arg));
                return null;
            }

            /// <inheritdoc />
            public override ComputeJobResultPolicy OnResult(
                IComputeJobResult<string> res, IList<IComputeJobResult<string>> rcvd)
            {
                // No need to synchronize accessing the counter since the jobs are submitted continuously one by one
                if (--_counter == 0)
                {
                    return ComputeJobResultPolicy.Reduce;
                }
                
                Send(new IgniteNameJob(_counter));

                return base.OnResult(res, rcvd);
            }

            /// <inheritdoc />
            public override string Reduce(IList<IComputeJobResult<string>> results) =>
                string.Join(" ", results.Select(i => i.Data));

            private void Send(IComputeJob<string> job)
            {
                if (_maybeTargetNode == null)
                {
                    _mapper.Send(job);
                }
                else
                {
                    _mapper.Send(job, _maybeTargetNode);
                }
            }
        }

        private class IgniteNameJob : ComputeJobAdapter<string>
        {
            [InstanceResource] private readonly IIgnite _ignite = null!;
                
            public IgniteNameJob(int arg) : base(arg) { }
            
            /// <inheritdoc />
            public override string Execute() => _ignite.GetCluster().GetLocalNode().ConsistentId.ToString();
        }

        private static IgniteConfiguration GetIgniteConfiguration(string igniteName, bool isClient = false) =>
            new IgniteConfiguration
            {
                ClientMode = isClient,
                ConsistentId = igniteName,
                IgniteInstanceName = igniteName,
                DiscoverySpi = GetStaticDiscovery(),
                JvmOptions = TestJavaOptions(),
                JvmClasspath = CreateTestClasspath()
            };
    }
}