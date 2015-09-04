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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Portable;
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
            IPortableObject taskArg = ToPortable(Grid1, new PortableTaskArgument(100));

            TestTask task = new TestTask(Grid1, taskArg);

            IPortableObject res = Grid1.Compute().Execute(task, taskArg);

            Assert.NotNull(res);

            Assert.AreEqual(400, res.Field<int>("val"));

            PortableTaskResult resObj = res.Deserialize<PortableTaskResult>();

            Assert.AreEqual(400, resObj.Val);
        }

        private static IPortableObject ToPortable(IIgnite grid, object obj)
        {
            var cache = grid.Cache<object, object>(Cache1Name).WithKeepPortable<object, object>();

            cache.Put(1, obj);

            return (IPortableObject) cache.Get(1);
        }

        /** <inheritDoc /> */
        override protected void PortableTypeConfigurations(ICollection<PortableTypeConfiguration> portTypeCfgs)
        {
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableJobArgument)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableJobResult)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableTaskArgument)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableTaskResult)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableJob)));
        }

        /// <summary>
        /// Test task.
        /// </summary>
        public class TestTask : ComputeTaskAdapter<IPortableObject, IPortableObject, IPortableObject>
        {
            /** */
            private readonly IIgnite _grid;

            private readonly IPortableObject _taskArgField;

            public TestTask(IIgnite grid, IPortableObject taskArgField)
            {
                _grid = grid;
                _taskArgField = taskArgField;
            }

            /** <inheritDoc /> */
            override public IDictionary<IComputeJob<IPortableObject>, IClusterNode> Map(IList<IClusterNode> subgrid, IPortableObject arg)
            {
                Assert.AreEqual(3, subgrid.Count);
                Assert.NotNull(_grid);

                IPortableObject taskArg = arg;

                CheckTaskArgument(taskArg);

                CheckTaskArgument(_taskArgField);

                IDictionary<IComputeJob<IPortableObject>, IClusterNode> jobs = new Dictionary<IComputeJob<IPortableObject>, IClusterNode>();


                foreach (IClusterNode node in subgrid)
                {
                    if (!Grid3Name.Equals(node.Attribute<string>("org.apache.ignite.ignite.name"))) // Grid3 does not have cache.
                    {
                        PortableJob job = new PortableJob();

                        job.Arg = ToPortable(_grid, new PortableJobArgument(200));

                        jobs.Add(job, node);
                    }
                }

                Assert.AreEqual(2, jobs.Count);

                return jobs;
            }

            private void CheckTaskArgument(IPortableObject taskArg)
            {
                Assert.IsNotNull(taskArg);

                Assert.AreEqual(100, taskArg.Field<int>("val"));

                PortableTaskArgument taskArgObj = taskArg.Deserialize<PortableTaskArgument>();

                Assert.AreEqual(100, taskArgObj.Val);
            }

            /** <inheritDoc /> */
            override public IPortableObject Reduce(IList<IComputeJobResult<IPortableObject>> results)
            {
                Assert.NotNull(_grid);

                Assert.AreEqual(2, results.Count);

                foreach (IComputeJobResult<IPortableObject> res in results)
                {
                    IPortableObject jobRes = res.Data();

                    Assert.NotNull(jobRes);

                    Assert.AreEqual(300, jobRes.Field<int>("val"));

                    PortableJobResult jobResObj = jobRes.Deserialize<PortableJobResult>();

                    Assert.AreEqual(300, jobResObj.Val);
                }

                return ToPortable(_grid, new PortableTaskResult(400));
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
        class PortableJob : IComputeJob<IPortableObject>
        {
            [InstanceResource]
            private IIgnite _grid = null;
            
            /** */
            public IPortableObject Arg;

            /** <inheritDoc /> */
            public IPortableObject Execute()
            {
                Assert.IsNotNull(Arg);

                Assert.AreEqual(200, Arg.Field<int>("val"));

                PortableJobArgument argObj = Arg.Deserialize<PortableJobArgument>();

                Assert.AreEqual(200, argObj.Val);

                return ToPortable(_grid, new PortableJobResult(300));
            }

            public void Cancel()
            {
                // No-op.
            }
        }
    }
}
