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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests class.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ComputeMultithreadedTest : AbstractTaskTest
    {
        /** */
        private static IList<Action<ICompute>> _actions;

        /// <summary>
        /// Constructor.
        /// </summary>
        public ComputeMultithreadedTest() : base(false) { }

        /// <summary>
        /// Set-up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _actions = new List<Action<ICompute>>
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
            _actions.Clear();
        }

        /// <summary>
        /// Test not-marshalable error occurred during map step.
        /// </summary>
        [Test]
        public void TestAllTaskTypeAtSameTime()
        {
            Assert.AreEqual(_actions.Count, 6);

            var compute = Grid1.Compute();

            TestUtils.RunMultiThreaded(() =>
            {
                _actions[TestUtils.Random.Next(_actions.Count)](compute);
            }, 4, 60);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType0()
        {
            Assert.AreEqual(_actions.Count, 6);

            TestUtils.RunMultiThreaded(() => _actions[0](Grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType1()
        {
            Assert.AreEqual(_actions.Count, 6);

            TestUtils.RunMultiThreaded(() => _actions[1](Grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType2()
        {
            Assert.AreEqual(_actions.Count, 6);

            TestUtils.RunMultiThreaded(() => _actions[2](Grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType3()
        {
            Assert.AreEqual(_actions.Count, 6);

            TestUtils.RunMultiThreaded(() => _actions[3](Grid1.Compute()), 4, 20);
        }
        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType4()
        {
            Assert.AreEqual(_actions.Count, 6);

            TestUtils.RunMultiThreaded(() => _actions[4](Grid1.Compute()), 4, 20);
        }

        /// <summary>
        ///
        /// </summary>
        [Test]
        public void TestSingleTaskType5()
        {
            Assert.AreEqual(_actions.Count, 6);

            TestUtils.RunMultiThreaded(() => _actions[5](Grid1.Compute()), 4, 20);
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
        private readonly string _s;

        /// <summary>
        ///
        /// </summary>
        /// <param name="s"></param>
        public MyNoArgClosure(string s)
        {
            _s = s;
        }

        /** <inheritDoc /> */
        public int Invoke()
        {
            return _s.Length;
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

            IClusterNode node = subgrid[TestUtils.Random.Next(subgrid.Count)];

            res.Add(job, node);

            return res;
        }

        /** <inheritDoc /> */
        public ComputeJobResultPolicy Result(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
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
        private string _s;

        /// <summary>
        ///
        /// </summary>
        /// <param name="s"></param>
        public StringLengthEmptyJob(string s)
        {
            _s = s;
        }

        /** <inheritDoc /> */
        public int Execute()
        {
            return _s.Length;
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
        private int _res;

        public bool Collect(int res)
        {
            _res += res;
            return true;
        }

        public int Reduce()
        {
            return _res;
        }
    }
}
