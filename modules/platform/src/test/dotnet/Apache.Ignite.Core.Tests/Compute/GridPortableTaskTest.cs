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
    public class GridPortableTaskTest : GridAbstractTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public GridPortableTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected GridPortableTaskTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestPortableObjectInTask()
        {
            IPortableObject taskArg = ToPortable(grid1, new PortableTaskArgument(100));

            TestTask task = new TestTask(grid1, taskArg);

            IPortableObject res = grid1.Compute().Execute(task, taskArg);

            Assert.NotNull(res);

            Assert.AreEqual(400, res.Field<int>("val"));

            PortableTaskResult resObj = res.Deserialize<PortableTaskResult>();

            Assert.AreEqual(400, resObj.val);
        }

        private static IPortableObject ToPortable(IIgnite grid, object obj)
        {
            var cache = grid.Cache<object, object>(CACHE1_NAME).WithKeepPortable<object, object>();

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
            private readonly IIgnite grid;

            private readonly IPortableObject taskArgField;

            public TestTask(IIgnite grid, IPortableObject taskArgField)
            {
                this.grid = grid;
                this.taskArgField = taskArgField;
            }

            /** <inheritDoc /> */
            override public IDictionary<IComputeJob<IPortableObject>, IClusterNode> Map(IList<IClusterNode> subgrid, IPortableObject arg)
            {
                Assert.AreEqual(3, subgrid.Count);
                Assert.NotNull(grid);

                IPortableObject taskArg = arg;

                CheckTaskArgument(taskArg);

                CheckTaskArgument(taskArgField);

                IDictionary<IComputeJob<IPortableObject>, IClusterNode> jobs = new Dictionary<IComputeJob<IPortableObject>, IClusterNode>();


                foreach (IClusterNode node in subgrid)
                {
                    if (!GRID3_NAME.Equals(node.Attribute<string>("org.apache.ignite.ignite.name"))) // Grid3 does not have cache.
                    {
                        PortableJob job = new PortableJob();

                        job.arg = ToPortable(grid, new PortableJobArgument(200));

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

                Assert.AreEqual(100, taskArgObj.val);
            }

            /** <inheritDoc /> */
            override public IPortableObject Reduce(IList<IComputeJobResult<IPortableObject>> results)
            {
                Assert.NotNull(grid);

                Assert.AreEqual(2, results.Count);

                foreach (IComputeJobResult<IPortableObject> res in results)
                {
                    IPortableObject jobRes = res.Data();

                    Assert.NotNull(jobRes);

                    Assert.AreEqual(300, jobRes.Field<int>("val"));

                    PortableJobResult jobResObj = jobRes.Deserialize<PortableJobResult>();

                    Assert.AreEqual(300, jobResObj.val);
                }

                return ToPortable(grid, new PortableTaskResult(400));
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableJobArgument
        {
            /** */
            public int val;

            public PortableJobArgument(int val)
            {
                this.val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableJobResult
        {
            /** */
            public int val;

            public PortableJobResult(int val)
            {
                this.val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableTaskArgument
        {
            /** */
            public int val;

            public PortableTaskArgument(int val)
            {
                this.val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableTaskResult
        {
            /** */
            public int val;

            public PortableTaskResult(int val)
            {
                this.val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class PortableJob : IComputeJob<IPortableObject>
        {
            [InstanceResource]
            private IIgnite grid = null;
            
            /** */
            public IPortableObject arg;

            /** <inheritDoc /> */
            public IPortableObject Execute()
            {
                Assert.IsNotNull(arg);

                Assert.AreEqual(200, arg.Field<int>("val"));

                PortableJobArgument argObj = arg.Deserialize<PortableJobArgument>();

                Assert.AreEqual(200, argObj.val);

                return ToPortable(grid, new PortableJobResult(300));
            }

            public void Cancel()
            {
                // No-op.
            }
        }
    }
}
