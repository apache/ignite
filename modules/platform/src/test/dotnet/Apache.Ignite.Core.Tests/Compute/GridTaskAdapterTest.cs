/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Compute
{
    using System;
    using System.Collections.Generic;

    using GridGain.Compute;
    using GridGain.Portable;
    using GridGain.Resource;
    
    using NUnit.Framework;    

    /// <summary>
    /// Test for task and job adapter.
    /// </summary>
    public class GridTaskAdapterTest : GridAbstractTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public GridTaskAdapterTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected GridTaskAdapterTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test for task adapter.
        /// </summary>
        [Test]
        public void TestTaskAdapter()
        {
            Assert.AreEqual(3, grid1.Cluster.Nodes().Count);

            HashSet<Guid> allNodes = new HashSet<Guid>(); 

            for (int i = 0; i < 20 && allNodes.Count < 3; i++)
            {
                HashSet<Guid> res = grid1.Compute().Execute(new TestSplitTask(), 1);

                Assert.AreEqual(1, res.Count);

                allNodes.UnionWith(res);
            }

            Assert.AreEqual(3, allNodes.Count);

            HashSet<Guid> res2 = grid1.Compute().Execute<int, Guid, HashSet<Guid>>(typeof(TestSplitTask), 3);

            Assert.IsTrue(res2.Count > 0);

            grid1.Compute().Execute(new TestSplitTask(), 100);

            Assert.AreEqual(3, allNodes.Count);
        }
        
        /// <summary>
        /// Test for job adapter.
        /// </summary>
        [Test]
        public void TestSerializableJobAdapter()
        {
            for (int i = 0; i < 10; i++)
            {
                bool res = grid1.Compute().Execute(new TestJobAdapterTask(), true);

                Assert.IsTrue(res);
            }
        }

        /// <summary>
        /// Test for job adapter.
        /// </summary>
        [Test]
        public void TestPortableJobAdapter()
        {
            for (int i = 0; i < 10; i++)
            {
                bool res = grid1.Compute().Execute(new TestJobAdapterTask(), false);

                Assert.IsTrue(res);
            }
        }

        /** <inheritDoc /> */
        override protected void PortableTypeConfigurations(ICollection<PortableTypeConfiguration> portTypeCfgs)
        {
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableJob)));
        }

        /// <summary>
        /// Test task.
        /// </summary>
        public class TestSplitTask : ComputeTaskSplitAdapter<int, Guid, HashSet<Guid>>
        {
            /** <inheritDoc /> */
            override protected ICollection<IComputeJob<Guid>> Split(int gridSize, int arg)
            {
                Assert.AreEqual(3, gridSize);

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
                    Guid id = res.Data();

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
                    jobs.Add(new PortableJob(100, "str"));

                return jobs;
            }

            /** <inheritDoc /> */
            override public bool Reduce(IList<IComputeJobResult<bool>> results)
            {
                Assert.AreEqual(1, results.Count);

                Assert.IsTrue(results[0].Data());

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
            private IGrid grid = null;

            /** <inheritDoc /> */
            public Guid Execute()
            {
                Assert.NotNull(grid);

                return grid.Cluster.LocalNode.Id;
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
            private IGrid grid = null;

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

                Assert.NotNull(grid);

                int arg1 = Argument<int>(0);

                Assert.AreEqual(100, arg1);

                string arg2 = Argument<string>(1);

                Assert.AreEqual("str", arg2);

                return true;
            }
        }

        /// <summary>
        /// Test portable job.
        /// </summary>
        public class PortableJob : ComputeJobAdapter<bool>
        {
            [InstanceResource]
            private IGrid grid = null;

            public PortableJob(params object[] args) : base(args)
            {
                // No-op.
            }

            /** <inheritDoc /> */
            override public bool Execute()
            {
                Assert.IsFalse(IsCancelled());

                Cancel();

                Assert.IsTrue(IsCancelled());

                Assert.NotNull(grid);

                int arg1 = Argument<int>(0);

                Assert.AreEqual(100, arg1);

                string arg2 = Argument<string>(1);

                Assert.AreEqual("str", arg2);

                return true;
            }
        }
    }
}
