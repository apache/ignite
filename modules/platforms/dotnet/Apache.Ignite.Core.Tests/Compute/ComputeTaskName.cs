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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;
    using static TestUtils;

    /// <summary>
    /// Tests for <see cref="IComputeTaskSession"/>
    /// </summary>
    public class ComputeTaskNameTest
    {
        /** First node. */
        private IIgnite _grid1;

        /** Second node. */
        private IIgnite _grid2;

        /** Third node. */
        private IIgnite _grid3;

        /** Thin client. */
        private IIgniteClient _igniteClient;

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitClient()
        {
            var configs = GetConfigs();

            _grid1 = Ignition.Start(Configuration(configs.Item1));
            _grid2 = Ignition.Start(Configuration(configs.Item2));

            AffinityTopologyVersion waitingTop = new AffinityTopologyVersion(2, 1);

            Assert.True(_grid1.WaitTopology(waitingTop), "Failed to wait topology " + waitingTop);

            _grid3 = Ignition.Start(Configuration(configs.Item3));

            // Start thin client.
            _igniteClient = Ignition.StartClient(GetThinClientConfiguration());
        }

        /// <summary>
        /// Gets the configs.
        /// </summary>
        protected virtual Tuple<string, string, string> GetConfigs()
        {
            var path = Path.Combine("Config", "Compute", "compute-grid");

            return Tuple.Create(path + "1.xml", path + "2.xml", path + "3.xml");
        }

        /// <summary>
        /// Gets the thin client configuration.
        /// </summary>
        private static IgniteClientConfiguration GetThinClientConfiguration()
        {
            return new IgniteClientConfiguration
            {
                Endpoints = new List<string> { IPAddress.Loopback.ToString() },
                SocketTimeout = TimeSpan.FromSeconds(15)
            };
        }

        /// <summary>
        /// .Net compute task name is written into java task. Checked in System Views (via SQL).
        /// </summary>
        [Test]
        public void TaskNameTakenFromPlatformTask()
        {
            // Call task asynchronously with delay
            var task = new LongTask(3000);
            _grid1.GetCompute().ExecuteAsync(task, 123);

            // Check task in system views via SQL
            var res = _grid1
                .GetOrCreateCache<string, string>("test")
                .Query(new SqlFieldsQuery("SELECT TASK_NAME, TASK_CLASS_NAME FROM SYS.TASKS", null))
                .GetAll()
                .Single();
            
            Assert.AreEqual("Apache.Ignite.Core.Tests.Compute.ComputeTaskNameTest+LongTask", res[0]);
            Assert.AreEqual("org.apache.ignite.internal.processors.platform.compute.PlatformFullTask", res[1]);
        }

        /// <summary>
        /// Create configuration.
        /// </summary>
        /// <param name="path">XML config path.</param>
        private static IgniteConfiguration Configuration(string path)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new BinaryBasicNameMapper { IsSimpleName = true }
                },
                SpringConfigUrl = path
            };
        }
        
        /// <summary>
        /// Creates a task that executes <see cref="LongJob"/>.
        /// </summary>
        [ComputeTaskSessionFullSupport]
        private class LongTask : ComputeTaskSplitAdapter<int, string, string>
        {
            private readonly int _delay;

            public LongTask(int delay)
            {
                _delay = delay;
            }

            /// <inheritdoc />
            public override string Reduce(IList<IComputeJobResult<string>> results) => results[0].Data;

            /// <inheritdoc />
            protected override ICollection<IComputeJob<string>> Split(int gridSize, int attrValue)
            {
                return new List<IComputeJob<string>> {new LongJob(_delay)};
            }
        }

        /// <summary>
        /// Implements delayed jow execution.
        /// </summary>
        private class LongJob : ComputeJobAdapter<string>
        {
            private readonly int _delay;

            public LongJob(int delay)
            {
                _delay = delay;
            }

            /// <inheritdoc />
            public override string Execute()
            {
                Thread.Sleep(_delay);
                return "OK";   
            }
        }
    }
}
