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
    using GridGain.Cluster;
    using GridGain.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests class.
    /// </summary>
    [Category(GridTestUtils.CATEGORY_INTENSIVE)]
    public class GridComputeMultithreadedTest : GridAbstractTaskTest
    {
        /** */
        private static IList<Action<ICompute>> ACTIONS;

        /// <summary>
        /// Constructor.
        /// </summary>
        public GridComputeMultithreadedTest() : base(false) { }

        /// <summary>
        /// Set-up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            ACTIONS = new List<Action<ICompute>>
            {
                compute => { compute.Apply(new My1ArgClosure(), "zzzz"); },
                compute => { compute.Broadcast(new My1ArgClosure(), "zzzz"); },
                compute => { compute.Broadcast(new MyNoArgClosure("zzzz")); },
                compute => { compute.Call(new MyNoArgClosure("zzzz")); },
                compute => { compute.Execute(new StringLengthEmptyTask(), "zzzz"); },
                compute =>
                {
                    compute.Apply(new My1ArgClosure(), new List<string> {"zzzz", "a", "b"}, new MyReducer());
                }
            };

        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            ACTIONS.Clear();
        }

        /// <summary>
        /// Test not-marshalable error occurred during map step.
        /// </summary>
        [Test]
        public void TestAllTaskTypeAtSameTime()
        {
            Assert.AreEqual(ACTIONS.Count, 6);

            var compute = grid1.Compute();

            GridTestUtils.RunMultiThreaded(() =>
            {
                ACTIONS[GridTestUtils.Random.Next(ACTIONS.Count)](compute);
            }, 4, 60);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType0()
        {
            Assert.AreEqual(ACTIONS.Count, 6);

            GridTestUtils.RunMultiThreaded(() => ACTIONS[0](grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType1()
        {
            Assert.AreEqual(ACTIONS.Count, 6);

            GridTestUtils.RunMultiThreaded(() => ACTIONS[1](grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType2()
        {
            Assert.AreEqual(ACTIONS.Count, 6);

            GridTestUtils.RunMultiThreaded(() => ACTIONS[2](grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType3()
        {
            Assert.AreEqual(ACTIONS.Count, 6);

            GridTestUtils.RunMultiThreaded(() => ACTIONS[3](grid1.Compute()), 4, 20);
        }
        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType4()
        {
            Assert.AreEqual(ACTIONS.Count, 6);

            GridTestUtils.RunMultiThreaded(() => ACTIONS[4](grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType5()
        {
            Assert.AreEqual(ACTIONS.Count, 6);

            GridTestUtils.RunMultiThreaded(() => ACTIONS[5](grid1.Compute()), 4, 20);
        }
    }

    /// <summary>
    /// Test class.
    /// </summary>
    [Serializable]
    public class My1ArgClosure : IComputeFunc<string, int>
    {
        /** <inheritDoc /> */
        public int Invoke(string s)
        {
            return s.Length;
        }
    }

    /// <summary>
    /// Test class.
    /// </summary>
    [Serializable]
    public class MyNoArgClosure : IComputeFunc<int>
    {
        /** */
        private readonly string s;

        /// <summary>
        ///
        /// </summary>
        /// <param name="s"></param>
        public MyNoArgClosure(string s)
        {
            this.s = s;
        }

        /** <inheritDoc /> */
        public int Invoke()
        {
            return s.Length;
        }
    }

    /// <summary>
    ///
    /// </summary>
    public class StringLengthEmptyTask : IComputeTask<string, int, int>
    {
        /** <inheritDoc /> */
        public IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, string arg)
        {
            var res = new Dictionary<IComputeJob<int>, IClusterNode>();

            var job = new StringLengthEmptyJob(arg);

            IClusterNode node = subgrid[GridTestUtils.Random.Next(subgrid.Count)];

            res.Add(job, node);

            return res;
        }

        /** <inheritDoc /> */
        public ComputeJobResultPolicy Result(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
        {
            return ComputeJobResultPolicy.WAIT;
        }

        /** <inheritDoc /> */
        public int Reduce(IList<IComputeJobResult<int>> results)
        {
            return results.Count == 0 ? 0 : results[0].Data();
        }
    }

    /// <summary>
    /// Test class.
    /// </summary>
    [Serializable]
    public class StringLengthEmptyJob: IComputeJob<int>
    {
        /** */
        private string s;

        /// <summary>
        ///
        /// </summary>
        /// <param name="s"></param>
        public StringLengthEmptyJob(string s)
        {
            this.s = s;
        }

        /** <inheritDoc /> */
        public int Execute()
        {
            return s.Length;
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            // No-op
        }
    }

    public class MyReducer : IComputeReducer<int, int>
    {
        /** */
        private int res;

        public bool Collect(int res)
        {
            this.res += res;
            return true;
        }

        public int Reduce()
        {
            return res;
        }
    }
}
