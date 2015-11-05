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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Task test result.
    /// </summary>
    public class PortableTaskTest : AbstractTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public PortableTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected PortableTaskTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestPortableObjectInTask()
        {
            var taskArg = new PortableWrapper {Item = ToPortable(Grid1, new PortableTaskArgument(100))};

            TestTask task = new TestTask(Grid1, taskArg);

            var res = Grid1.GetCompute().Execute(task, taskArg).Item;

            Assert.NotNull(res);

            Assert.AreEqual(400, res.GetField<int>("val"));

            PortableTaskResult resObj = res.Deserialize<PortableTaskResult>();

            Assert.AreEqual(400, resObj.Val);
        }

        private static IBinaryObject ToPortable(IIgnite grid, object obj)
        {
            var cache = grid.GetCache<object, object>(Cache1Name).WithKeepPortable<object, object>();

            cache.Put(1, obj);

            return (IBinaryObject) cache.Get(1);
        }

        /** <inheritDoc /> */
        override protected void PortableTypeConfigurations(ICollection<BinaryTypeConfiguration> portTypeCfgs)
        {
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(PortableJobArgument)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(PortableJobResult)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(PortableTaskArgument)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(PortableTaskResult)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(PortableJob)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(PortableWrapper)));
        }

        /// <summary>
        /// Test task.
        /// </summary>
        class TestTask : ComputeTaskAdapter<PortableWrapper, PortableWrapper, PortableWrapper>
        {
            /** */
            private readonly IIgnite _grid;

            private readonly PortableWrapper _taskArgField;

            public TestTask(IIgnite grid, PortableWrapper taskArgField)
            {
                _grid = grid;
                _taskArgField = taskArgField;
            }

            /** <inheritDoc /> */
            override public IDictionary<IComputeJob<PortableWrapper>, IClusterNode> Map(IList<IClusterNode> subgrid, PortableWrapper arg)
            {
                Assert.AreEqual(3, subgrid.Count);
                Assert.NotNull(_grid);

                var taskArg = arg;

                CheckTaskArgument(taskArg);

                CheckTaskArgument(_taskArgField);

                var jobs = new Dictionary<IComputeJob<PortableWrapper>, IClusterNode>();


                foreach (IClusterNode node in subgrid)
                {
                    if (!Grid3Name.Equals(node.GetAttribute<string>("org.apache.ignite.ignite.name"))) // Grid3 does not have cache.
                    {
                        var job = new PortableJob
                        {
                            Arg = new PortableWrapper {Item = ToPortable(_grid, new PortableJobArgument(200))}
                        };

                        jobs.Add(job, node);
                    }
                }

                Assert.AreEqual(2, jobs.Count);

                return jobs;
            }

            private void CheckTaskArgument(PortableWrapper arg)
            {
                Assert.IsNotNull(arg);
                
                var taskArg = arg.Item;

                Assert.IsNotNull(taskArg);

                Assert.AreEqual(100, taskArg.GetField<int>("val"));

                PortableTaskArgument taskArgObj = taskArg.Deserialize<PortableTaskArgument>();

                Assert.AreEqual(100, taskArgObj.Val);
            }

            /** <inheritDoc /> */
            override public PortableWrapper Reduce(IList<IComputeJobResult<PortableWrapper>> results)
            {
                Assert.NotNull(_grid);

                Assert.AreEqual(2, results.Count);

                foreach (var res in results)
                {
                    var jobRes = res.Data.Item;

                    Assert.NotNull(jobRes);

                    Assert.AreEqual(300, jobRes.GetField<int>("val"));

                    PortableJobResult jobResObj = jobRes.Deserialize<PortableJobResult>();

                    Assert.AreEqual(300, jobResObj.Val);
                }

                return new PortableWrapper {Item = ToPortable(_grid, new PortableTaskResult(400))};
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableJobArgument
        {
            /** */
            public int Val;

            public PortableJobArgument(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableJobResult
        {
            /** */
            public int Val;

            public PortableJobResult(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableTaskArgument
        {
            /** */
            public int Val;

            public PortableTaskArgument(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableTaskResult
        {
            /** */
            public int Val;

            public PortableTaskResult(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableJob : IComputeJob<PortableWrapper>
        {
            [InstanceResource]
            private IIgnite _grid = null;
            
            /** */
            public PortableWrapper Arg;

            /** <inheritDoc /> */

            public PortableWrapper Execute()
            {
                Assert.IsNotNull(Arg);

                var arg = Arg.Item;

                Assert.IsNotNull(arg);

                Assert.AreEqual(200, arg.GetField<int>("val"));

                PortableJobArgument argObj = arg.Deserialize<PortableJobArgument>();

                Assert.AreEqual(200, argObj.Val);

                return new PortableWrapper {Item = ToPortable(_grid, new PortableJobResult(300))};
            }

            public void Cancel()
            {
                // No-op.
            }
        }

        class PortableWrapper
        {
            public IBinaryObject Item { get; set; }
        }
    }
}
