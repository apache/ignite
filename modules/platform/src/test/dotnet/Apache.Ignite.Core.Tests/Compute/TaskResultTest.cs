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
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests task result.
    /// </summary>
    public class TaskResultTest : AbstractTaskTest
    {
        /** Grid name. */
        private static volatile string _gridName;

        /// <summary>
        /// Constructor.
        /// </summary>
        public TaskResultTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="forked">Fork flag.</param>
        protected TaskResultTest(bool forked) : base(forked) { }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestTaskResultInt()
        {
            TestTask<int> task = new TestTask<int>();

            int res = Grid1.Compute().Execute(task, new Tuple<bool, int>(true, 10));

            Assert.AreEqual(10, res);

            res = Grid1.Compute().Execute(task, new Tuple<bool, int>(false, 11));

            Assert.AreEqual(11, res);
        }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestTaskResultLong()
        {
            TestTask<long> task = new TestTask<long>();

            long res = Grid1.Compute().Execute(task, new Tuple<bool, long>(true, 10000000000));

            Assert.AreEqual(10000000000, res);

            res = Grid1.Compute().Execute(task, new Tuple<bool, long>(false, 10000000001));

            Assert.AreEqual(10000000001, res);
        }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestTaskResultFloat()
        {
            TestTask<float> task = new TestTask<float>();

            float res = Grid1.Compute().Execute(task, new Tuple<bool, float>(true, 1.1f));

            Assert.AreEqual(1.1f, res);

            res = Grid1.Compute().Execute(task, new Tuple<bool, float>(false, -1.1f));

            Assert.AreEqual(-1.1f, res);
        }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestTaskResultPortable()
        {
            TestTask<PortableResult> task = new TestTask<PortableResult>();

            PortableResult val = new PortableResult(100);

            PortableResult res = Grid1.Compute().Execute(task, new Tuple<bool, PortableResult>(true, val));

            Assert.AreEqual(val.Val, res.Val);

            val.Val = 101;

            res = Grid1.Compute().Execute(task, new Tuple<bool, PortableResult>(false, val));

            Assert.AreEqual(val.Val, res.Val);
        }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestTaskResultSerializable()
        {
            TestTask<SerializableResult> task = new TestTask<SerializableResult>();

            SerializableResult val = new SerializableResult(100);

            SerializableResult res = Grid1.Compute().Execute(task, new Tuple<bool, SerializableResult>(true, val));

            Assert.AreEqual(val.Val, res.Val);

            val.Val = 101;

            res = Grid1.Compute().Execute(task, new Tuple<bool, SerializableResult>(false, val));

            Assert.AreEqual(val.Val, res.Val);
        }

        /// <summary>
        /// Test for task result.
        /// </summary>
        [Test]
        public void TestTaskResultLarge()
        {
            TestTask<byte[]> task = new TestTask<byte[]>();

            byte[] res = Grid1.Compute().Execute(task,
                new Tuple<bool, byte[]>(true, new byte[100 * 1024]));

            Assert.AreEqual(100 * 1024, res.Length);

            res = Grid1.Compute().Execute(task, new Tuple<bool, byte[]>(false, new byte[101 * 1024]));

            Assert.AreEqual(101 * 1024, res.Length);
        }

        /** <inheritDoc /> */
        override protected void PortableTypeConfigurations(ICollection<PortableTypeConfiguration> portTypeCfgs)
        {
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableResult)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(TestPortableJob)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableOutFunc)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableFunc)));
        }

        [Test]
        public void TestOutFuncResultPrimitive1()
        {
            ICollection<int> res = Grid1.Compute().Broadcast(new PortableOutFunc());

            Assert.AreEqual(3, res.Count);

            foreach (int r in res)
                Assert.AreEqual(10, r);
        }

        [Test]
        public void TestOutFuncResultPrimitive2()
        {
            ICollection<int> res = Grid1.Compute().Broadcast(new SerializableOutFunc());

            Assert.AreEqual(3, res.Count);

            foreach (int r in res)
                Assert.AreEqual(10, r);
        }

        [Test]
        public void TestFuncResultPrimitive1()
        {
            ICollection<int> res = Grid1.Compute().Broadcast(new PortableFunc(), 10);

            Assert.AreEqual(3, res.Count);

            foreach (int r in res)
                Assert.AreEqual(11, r);
        }

        [Test]
        public void TestFuncResultPrimitive2()
        {
            ICollection<int> res = Grid1.Compute().Broadcast(new SerializableFunc(), 10);

            Assert.AreEqual(3, res.Count);

            foreach (int r in res)
                Assert.AreEqual(11, r);
        }

        interface IUserInterface<in T, out TR>
        {
            TR Invoke(T arg);
        }

        /// <summary>
        /// Test function.
        /// </summary>
        public class PortableFunc : IComputeFunc<int, int>, IUserInterface<int, int>
        {
            int IComputeFunc<int, int>.Invoke(int arg)
            {
                return arg + 1;
            }

            int IUserInterface<int, int>.Invoke(int arg)
            {
                // Same signature as IComputeFunc<int, int>, but from different interface
                throw new Exception("Invalid method");
            }

            public int Invoke(int arg)
            {
                // Same signature as IComputeFunc<int, int>, 
                // but due to explicit interface implementation this is a wrong method
                throw new Exception("Invalid method");
            }
        }

        /// <summary>
        /// Test function.
        /// </summary>
        [Serializable]
        public class SerializableFunc : IComputeFunc<int, int>
        {
            public int Invoke(int arg)
            {
                return arg + 1;
            }
        }

        /// <summary>
        /// Test function.
        /// </summary>
        public class PortableOutFunc : IComputeFunc<int>
        {
            public int Invoke()
            {
                return 10;
            }
        }

        /// <summary>
        /// Test function.
        /// </summary>
        [Serializable]
        public class SerializableOutFunc : IComputeFunc<int>
        {
            public int Invoke()
            {
                return 10;
            }
        }

        /// <summary>
        /// Test task.
        /// </summary>
        public class TestTask<T> : ComputeTaskAdapter<Tuple<bool, T>, T, T>
        {
            /** <inheritDoc /> */
            override public IDictionary<IComputeJob<T>, IClusterNode> Map(IList<IClusterNode> subgrid, Tuple<bool, T> arg)
            {
                _gridName = null;

                Assert.AreEqual(3, subgrid.Count);

                bool local = arg.Item1;
                T res = arg.Item2;

                var jobs = new Dictionary<IComputeJob<T>, IClusterNode>();

                IComputeJob<T> job;

                if (res is PortableResult)
                {
                    TestPortableJob job0 = new TestPortableJob();

                    job0.SetArguments(res);

                    job = (IComputeJob<T>) job0;
                }
                else
                {
                    TestJob<T> job0 = new TestJob<T>();

                    job0.SetArguments(res);

                    job = job0;
                }

                foreach (IClusterNode node in subgrid)
                {
                    bool add = local ? node.IsLocal : !node.IsLocal;

                    if (add)
                    {
                        jobs.Add(job, node);

                        break;
                    }
                }

                Assert.AreEqual(1, jobs.Count);

                return jobs;
            }

            /** <inheritDoc /> */
            override public T Reduce(IList<IComputeJobResult<T>> results)
            {
                Assert.AreEqual(1, results.Count);

                var res = results[0];

                Assert.IsNull(res.Exception());

                Assert.IsFalse(res.Cancelled);

                Assert.IsNotNull(_gridName);

                Assert.AreEqual(GridId(_gridName), res.NodeId);

                var job = res.Job();

                Assert.IsNotNull(job);

                return res.Data();
            }
        }

        private static Guid GridId(string gridName)
        {
            if (gridName.Equals(Grid1Name))
                return Ignition.GetIgnite(Grid1Name).Cluster.LocalNode.Id;
            if (gridName.Equals(Grid2Name))
                return Ignition.GetIgnite(Grid2Name).Cluster.LocalNode.Id;
            if (gridName.Equals(Grid3Name))
                return Ignition.GetIgnite(Grid3Name).Cluster.LocalNode.Id;

            Assert.Fail("Failed to find grid " + gridName);

            return new Guid();
        }

        /// <summary>
        ///
        /// </summary>
        class PortableResult
        {
            /** */
            public int Val;

            public PortableResult(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        class SerializableResult
        {
            /** */
            public int Val;

            public SerializableResult(int val)
            {
                Val = val;
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        class TestJob<T> : ComputeJobAdapter<T>
        {
            [InstanceResource]
            private IIgnite _grid = null;

            /** <inheritDoc /> */
            override public T Execute()
            {
                Assert.IsNotNull(_grid);

                _gridName = _grid.Name;

                T res = Argument<T>(0);

                return res;
            }
        }

        /// <summary>
        ///
        /// </summary>
        class TestPortableJob : ComputeJobAdapter<PortableResult>
        {
            [InstanceResource]
            private IIgnite _grid = null;

            /** <inheritDoc /> */
            override public PortableResult Execute()
            {
                Assert.IsNotNull(_grid);

                _gridName = _grid.Name;

                PortableResult res = Argument<PortableResult>(0);

                return res;
            }
        }
    }
}
