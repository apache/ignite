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
    using System.Linq;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Test for task and job adapter.
    /// </summary>
    public class FailoverTaskSelfTest : AbstractTaskTest
    {
        /** */
        static volatile string _gridName;

        /** */
        static volatile int _cnt;

        /// <summary>
        /// Constructor.
        /// </summary>
        public FailoverTaskSelfTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected FailoverTaskSelfTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test for GridComputeJobFailoverException.
        /// </summary>
        [Test]
        public void TestClosureFailoverException()
        {
            for (int i = 0; i < 20; i++)
            {
                int res = Grid1.GetCompute().Call(new TestClosure());

                Assert.AreEqual(2, res);

                Cleanup();
            }
        }

        /// <summary>
        /// Test for GridComputeJobFailoverException with serializable job.
        /// </summary>
        [Test]
        public void TestTaskAdapterFailoverExceptionSerializable()
        {
            TestTaskAdapterFailoverException(true);
        }

        /// <summary>
        /// Test for GridComputeJobFailoverException with binary job.
        /// </summary>
        [Test]
        public void TestTaskAdapterFailoverExceptionBinarizable()
        {
            TestTaskAdapterFailoverException(false);
        }

        /// <summary>
        /// Test for GridComputeJobFailoverException.
        /// </summary>
        private void TestTaskAdapterFailoverException(bool serializable)
        {
            int res = Grid1.GetCompute().Execute(new TestTask(),
                new Tuple<bool, bool>(serializable, true));

            Assert.AreEqual(2, res);

            Cleanup();

            res = Grid1.GetCompute().Execute(new TestTask(),
                new Tuple<bool, bool>(serializable, false));

            Assert.AreEqual(2, res);
        }

        /// <summary>
        /// Cleanup.
        /// </summary>
        [TearDown]
        public void Cleanup()
        {
            _cnt = 0;

            _gridName = null;
        }

        /// <summary>
        /// Test task.
        /// </summary>
        private class TestTask : ComputeTaskAdapter<Tuple<bool, bool>, int, int>
        {
            /** <inheritDoc /> */
            public override IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, 
                Tuple<bool, bool> arg)
            {
                Assert.AreEqual(2, subgrid.Count);

                var serializable = arg.Item1;
                var local = arg.Item2;

                var job = serializable 
                    ? (IComputeJob<int>) new TestSerializableJob() 
                    :  new TestBinarizableJob();

                var node = subgrid.Single(x => x.IsLocal == local);

                return new Dictionary<IComputeJob<int>, IClusterNode> {{job, node}};
            }

            /** <inheritDoc /> */
            public override int Reduce(IList<IComputeJobResult<int>> results)
            {
                Assert.AreEqual(1, results.Count);

                return results[0].Data;
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class TestClosure : IComputeFunc<int>
        {
            [InstanceResource]
            private readonly IIgnite _grid = null;

            /** <inheritDoc /> */
            public int Invoke()
            {
                return FailoverJob(_grid);
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        private class TestSerializableJob : IComputeJob<int>
        {
            [InstanceResource]
            private readonly IIgnite _grid = null;

            /** <inheritDoc /> */
            public int Execute()
            {
                return FailoverJob(_grid);
            }

            /** <inheritDoc /> */
            public void Cancel()
            {
                // No-op.
            }
        }

        /// <summary>
        ///
        /// </summary>
        private class TestBinarizableJob : IComputeJob<int>
        {
            [InstanceResource]
            private readonly IIgnite _grid = null;

            /** <inheritDoc /> */
            public int Execute()
            {
                return FailoverJob(_grid);
            }

            public void Cancel()
            {
                // No-op.
            }
        }

        /// <summary>
        /// Throws GridComputeJobFailoverException on first call.
        /// </summary>
        private static int FailoverJob(IIgnite grid)
        {
            Assert.NotNull(grid);

            _cnt++;

            if (_gridName == null)
            {
                _gridName = grid.Name;

                throw new ComputeJobFailoverException("Test error.");
            }
            Assert.AreNotEqual(_gridName, grid.Name);

            return _cnt;
        }
    }
}
