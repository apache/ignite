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
    public class BinarizableTaskTest : AbstractTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public BinarizableTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected BinarizableTaskTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestBinarizableObjectInTask()
        {
            var taskArg = new BinarizableWrapper {Item = ToBinary(Grid1, new BinarizableTaskArgument(100))};

            TestTask task = new TestTask(Grid1, taskArg);

            var res = Grid1.GetCompute().Execute(task, taskArg).Item;

            Assert.NotNull(res);

            Assert.AreEqual(400, res.GetField<int>("val"));

            BinarizableTaskResult resObj = res.Deserialize<BinarizableTaskResult>();

            Assert.AreEqual(400, resObj.Val);
        }

        private static IBinaryObject ToBinary(IIgnite grid, object obj)
        {
            var cache = grid.GetCache<object, object>(Cache1Name).WithKeepBinary<object, object>();

            cache.Put(1, obj);

            return (IBinaryObject) cache.Get(1);
        }

        /** <inheritDoc /> */
        override protected void GetBinaryTypeConfigurations(ICollection<BinaryTypeConfiguration> portTypeCfgs)
        {
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(BinarizableJobArgument)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(BinarizableJobResult)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(BinarizableTaskArgument)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(BinarizableTaskResult)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(BinarizableJob)));
            portTypeCfgs.Add(new BinaryTypeConfiguration(typeof(BinarizableWrapper)));
        }

        /// <summary>
        /// Test task.
        /// </summary>
        class TestTask : ComputeTaskAdapter<BinarizableWrapper, BinarizableWrapper, BinarizableWrapper>
        {
            /** */
            private readonly IIgnite _grid;

            private readonly BinarizableWrapper _taskArgField;

            public TestTask(IIgnite grid, BinarizableWrapper taskArgField)
            {
                _grid = grid;
                _taskArgField = taskArgField;
            }

            /** <inheritDoc /> */
            override public IDictionary<IComputeJob<BinarizableWrapper>, IClusterNode> Map(IList<IClusterNode> subgrid, BinarizableWrapper arg)
            {
                Assert.AreEqual(2, subgrid.Count);
                Assert.NotNull(_grid);

                var taskArg = arg;

                CheckTaskArgument(taskArg);

                CheckTaskArgument(_taskArgField);

                var jobs = new Dictionary<IComputeJob<BinarizableWrapper>, IClusterNode>();


                foreach (IClusterNode node in subgrid)
                {
                    var job = new BinarizableJob
                    {
                        Arg = new BinarizableWrapper {Item = ToBinary(_grid, new BinarizableJobArgument(200))}
                    };

                    jobs.Add(job, node);
                }

                Assert.AreEqual(2, jobs.Count);

                return jobs;
            }

            private void CheckTaskArgument(BinarizableWrapper arg)
            {
                Assert.IsNotNull(arg);
                
                var taskArg = arg.Item;

                Assert.IsNotNull(taskArg);

                Assert.AreEqual(100, taskArg.GetField<int>("val"));

                BinarizableTaskArgument taskArgObj = taskArg.Deserialize<BinarizableTaskArgument>();

                Assert.AreEqual(100, taskArgObj.Val);
            }

            /** <inheritDoc /> */
            override public BinarizableWrapper Reduce(IList<IComputeJobResult<BinarizableWrapper>> results)
            {
                Assert.NotNull(_grid);

                Assert.AreEqual(2, results.Count);

                foreach (var res in results)
                {
                    var jobRes = res.Data.Item;

                    Assert.NotNull(jobRes);

                    Assert.AreEqual(300, jobRes.GetField<int>("val"));

                    BinarizableJobResult jobResObj = jobRes.Deserialize<BinarizableJobResult>();

                    Assert.AreEqual(300, jobResObj.Val);
                }

                return new BinarizableWrapper {Item = ToBinary(_grid, new BinarizableTaskResult(400))};
            }
        }

        /// <summary>
        ///
        /// </summary>
        class BinarizableJobArgument
        {
            /** */
            public readonly int Val;

            public BinarizableJobArgument(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class BinarizableJobResult
        {
            /** */
            public readonly int Val;

            public BinarizableJobResult(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class BinarizableTaskArgument
        {
            /** */
            public readonly int Val;

            public BinarizableTaskArgument(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class BinarizableTaskResult
        {
            /** */
            public readonly int Val;

            public BinarizableTaskResult(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class BinarizableJob : IComputeJob<BinarizableWrapper>
        {
            [InstanceResource]
            private readonly IIgnite _grid = null;
            
            /** */
            public BinarizableWrapper Arg;

            /** <inheritDoc /> */

            public BinarizableWrapper Execute()
            {
                Assert.IsNotNull(Arg);

                var arg = Arg.Item;

                Assert.IsNotNull(arg);

                Assert.AreEqual(200, arg.GetField<int>("val"));

                var argObj = arg.Deserialize<BinarizableJobArgument>();

                Assert.AreEqual(200, argObj.Val);

                return new BinarizableWrapper {Item = ToBinary(_grid, new BinarizableJobResult(300))};
            }

            public void Cancel()
            {
                // No-op.
            }
        }

        class BinarizableWrapper
        {
            public IBinaryObject Item { get; set; }
        }
    }
}
