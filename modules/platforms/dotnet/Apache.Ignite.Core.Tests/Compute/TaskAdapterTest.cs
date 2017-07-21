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

#pragma warning disable 649
#pragma warning disable 169
namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Test for task and job adapter.
    /// </summary>
    public class TaskAdapterTest : AbstractTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public TaskAdapterTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected TaskAdapterTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test for task adapter.
        /// </summary>
        [Test]
        public void TestTaskAdapter()
        {
            Assert.AreEqual(3, Grid1.GetCluster().GetNodes().Count);

            HashSet<Guid> allNodes = new HashSet<Guid>(); 

            for (int i = 0; i < 20 && allNodes.Count < GetServerCount(); i++)
            {
                HashSet<Guid> res = Grid1.GetCompute().Execute(new TestSplitTask(), 1);

                Assert.AreEqual(1, res.Count);

                allNodes.UnionWith(res);
            }

            Assert.AreEqual(GetServerCount(), allNodes.Count);

            HashSet<Guid> res2 = Grid1.GetCompute().Execute<int, Guid, HashSet<Guid>>(typeof(TestSplitTask), 3);

            Assert.IsTrue(res2.Count > 0);

            Grid1.GetCompute().Execute(new TestSplitTask(), 100);

            Assert.AreEqual(GetServerCount(), allNodes.Count);
        }
        
        /// <summary>
        /// Test for job adapter.
        /// </summary>
        [Test]
        public void TestSerializableJobAdapter()
        {
            for (int i = 0; i < 10; i++)
            {
                bool res = Grid1.GetCompute().Execute(new TestJobAdapterTask(), true);

                Assert.IsTrue(res);
            }
        }

        /// <summary>
        /// Test for job adapter.
        /// </summary>
        [Test]
        public void TestBinarizableJobAdapter()
        {
            for (int i = 0; i < 10; i++)
            {
                bool res = Grid1.GetCompute().Execute(new TestJobAdapterTask(), false);

                Assert.IsTrue(res);
            }
        }

        /// <summary>
        /// Test task.
        /// </summary>
        public class TestSplitTask : ComputeTaskSplitAdapter<int, Guid, HashSet<Guid>>
        {
            [InstanceResource]
            private readonly IIgnite _ignite;

            /** <inheritDoc /> */
            override protected ICollection<IComputeJob<Guid>> Split(int gridSize, int arg)
            {
                Assert.AreEqual(_ignite.GetCluster().GetNodes().Count(x => !x.IsClient), gridSize);

                int jobsNum = arg;

                Assert.IsTrue(jobsNum > 0);

                var jobs = new List<IComputeJob<Guid>>(jobsNum);

                for (int i = 0; i < jobsNum; i++)
                    jobs.Add(new NodeIdJob());

                return jobs;
            }

            /** <inheritDoc /> */
            override public HashSet<Guid> Reduce(IList<IComputeJobResult<Guid>> results)
            {
                HashSet<Guid> nodes = new HashSet<Guid>();

                foreach (var res in results) {
                    Guid id = res.Data;

                    Assert.NotNull(id);

                    nodes.Add(id);
                }

                return nodes;
            }
        }

        /// <summary>
        /// Test task.
        /// </summary>
        public class TestJobAdapterTask : ComputeTaskSplitAdapter<bool, bool, bool>
        {
            /** <inheritDoc /> */
            override protected ICollection<IComputeJob<bool>> Split(int gridSize, bool arg)
            {
                bool serializable = arg;

                ICollection<IComputeJob<bool>> jobs = new List<IComputeJob<bool>>(1);

                if (serializable)
                    jobs.Add(new SerializableJob(100, "str"));
                else
                    jobs.Add(new BinarizableJob(100, "str"));

                return jobs;
            }

            /** <inheritDoc /> */
            override public bool Reduce(IList<IComputeJobResult<bool>> results)
            {
                Assert.AreEqual(1, results.Count);

                Assert.IsTrue(results[0].Data);

                return true;
            }
        }

        /// <summary>
        /// Test job.
        /// </summary>
        [Serializable]
        public class NodeIdJob : IComputeJob<Guid>
        {
            [InstanceResource]
            private IIgnite _grid = null;

            /** <inheritDoc /> */
            public Guid Execute()
            {
                Assert.NotNull(_grid);

                return _grid.GetCluster().GetLocalNode().Id;
            }

            /** <inheritDoc /> */
            public void Cancel()
            {
                // No-op.
            }
        }

        /// <summary>
        /// Test serializable job.
        /// </summary>
        [Serializable]
        public class SerializableJob : ComputeJobAdapter<bool>
        {
            [InstanceResource]
            private IIgnite _grid = null;

            public SerializableJob(params object[] args) : base(args)
            { 
                // No-op.
            }

            /** <inheritDoc /> */
            override public bool Execute()
            {
                Assert.IsFalse(IsCancelled());

                Cancel();

                Assert.IsTrue(IsCancelled());

                Assert.NotNull(_grid);

                int arg1 = GetArgument<int>(0);

                Assert.AreEqual(100, arg1);

                string arg2 = GetArgument<string>(1);

                Assert.AreEqual("str", arg2);

                return true;
            }
        }

        /// <summary>
        /// Test binary job.
        /// </summary>
        public class BinarizableJob : ComputeJobAdapter<bool>
        {
            [InstanceResource]
            private IIgnite _grid;

            public BinarizableJob(params object[] args) : base(args)
            {
                // No-op.
            }

            /** <inheritDoc /> */
            override public bool Execute()
            {
                Assert.IsFalse(IsCancelled());

                Cancel();

                Assert.IsTrue(IsCancelled());

                Assert.NotNull(_grid);

                int arg1 = GetArgument<int>(0);

                Assert.AreEqual(100, arg1);

                string arg2 = GetArgument<string>(1);

                Assert.AreEqual("str", arg2);

                return true;
            }
        }
    }
}
