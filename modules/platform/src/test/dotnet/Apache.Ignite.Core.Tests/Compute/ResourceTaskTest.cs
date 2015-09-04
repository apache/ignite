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
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Test resource injections in tasks and jobs.
    /// </summary>
    public class ResourceTaskTest : AbstractTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public ResourceTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected ResourceTaskTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test Ignite injection into the task.
        /// </summary>
        [Test]
        public void TestTaskInjection()
        {
            int res = Grid1.Compute().Execute(new InjectionTask(), 0);

            Assert.AreEqual(Grid1.Cluster.Nodes().Count, res);
        }

        /// <summary>
        /// Test Ignite injection into the closure.
        /// </summary>
        [Test]
        public void TestClosureInjection()
        {
            var res = Grid1.Compute().Broadcast(new InjectionClosure(), 1);

            Assert.AreEqual(Grid1.Cluster.Nodes().Count, res.Sum());
        }

        /// <summary>
        /// Test Ignite injection into reducer.
        /// </summary>
        [Test]
        public void TestReducerInjection()
        {
            int res = Grid1.Compute().Apply(new InjectionClosure(), new List<int> { 1, 1, 1 }, new InjectionReducer());

            Assert.AreEqual(Grid1.Cluster.Nodes().Count, res);
        }

        /// <summary>
        /// Test no-result-cache attribute.
        /// </summary>
        [Test]
        public void TestNoResultCache()
        {
            int res = Grid1.Compute().Execute(new NoResultCacheTask(), 0);

            Assert.AreEqual(Grid1.Cluster.Nodes().Count, res);
        }

        /// <summary>
        /// Injection task.
        /// </summary>
        public class InjectionTask : Injectee, IComputeTask<object, int, int>
        {
            /** <inheritDoc /> */
            public IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
            {
                CheckInjection();

                return subgrid.ToDictionary(x => (IComputeJob<int>) new InjectionJob(), x => x);
            }

            /** <inheritDoc /> */
            public ComputeJobResultPolicy Result(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
            {
                return ComputeJobResultPolicy.Wait;
            }

            /** <inheritDoc /> */
            public int Reduce(IList<IComputeJobResult<int>> results)
            {
                return results.Sum(res => res.Data());
            }
        }

        /// <summary>
        /// Injection job.
        /// </summary>
        [Serializable]
        public class InjectionJob : Injectee, IComputeJob<int>
        {
            /// <summary>
            ///
            /// </summary>
            public InjectionJob()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public InjectionJob(SerializationInfo info, StreamingContext context) : base(info, context)
            {
                // No-op.
            }

            /** <inheritDoc /> */
            public int Execute()
            {
                CheckInjection();

                return 1;
            }

            public void Cancel()
            {
                // No-op.
            }
        }

        /// <summary>
        /// Injection closure.
        /// </summary>
        [Serializable]
        public class InjectionClosure : IComputeFunc<int, int>
        {
            /** */
            [InstanceResource]
            private static IIgnite _staticGrid1;

            /** */
            [InstanceResource]
            public static IIgnite StaticGrid2;

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            public static IIgnite StaticPropGrid1
            {
                get { return _staticGrid1; }
                set { _staticGrid1 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            private static IIgnite StaticPropGrid2
            {
                get { return StaticGrid2; }
                set { StaticGrid2 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            public static void StaticMethod1(IIgnite grid)
            {
                _staticGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private static void StaticMethod2(IIgnite grid)
            {
                StaticGrid2 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            public InjectionClosure()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public InjectionClosure(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }

            /** */
            [InstanceResource]
            private readonly IIgnite _grid1 = null;

            /** */
            [InstanceResource]
            public IIgnite Grid2;

            /** */
            private IIgnite _mthdGrid1;

            /** */
            private IIgnite _mthdGrid2;

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            public IIgnite PropGrid1
            {
                get;
                set;
            }

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            private IIgnite PropGrid2
            {
                get;
                set;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            public void Method1(IIgnite grid)
            {
                _mthdGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private void Method2(IIgnite grid)
            {
                _mthdGrid2 = grid;
            }

            /// <summary>
            /// Check Ignite injections.
            /// </summary>
            protected void CheckInjection()
            {
                Assert.IsTrue(_staticGrid1 == null);
                Assert.IsTrue(StaticGrid2 == null);

                Assert.IsTrue(_grid1 != null);
                Assert.IsTrue(Grid2 == _grid1);

                Assert.IsTrue(PropGrid1 == _grid1);
                Assert.IsTrue(PropGrid2 == _grid1);

                Assert.IsTrue(_mthdGrid1 == _grid1);
                Assert.IsTrue(_mthdGrid2 == _grid1);
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }

            /** <inheritDoc /> */
            public int Invoke(int arg)
            {
                CheckInjection();

                return arg;
            }
        }

        /// <summary>
        /// Injection reducer.
        /// </summary>
        public class InjectionReducer : Injectee, IComputeReducer<int, int>
        {
            /** Collected results. */
            private readonly ICollection<int> _ress = new List<int>();

            /** <inheritDoc /> */
            public bool Collect(int res)
            {
                CheckInjection();

                lock (_ress)
                {
                    _ress.Add(res);
                }

                return true;
            }

            /** <inheritDoc /> */
            public int Reduce()
            {
                CheckInjection();

                lock (_ress)
                {
                    return _ress.Sum();
                }
            }
        }

        /// <summary>
        /// Injectee.
        /// </summary>
        [Serializable]
        public class Injectee : ISerializable
        {
            /** */
            [InstanceResource]
            private static IIgnite _staticGrid1;

            /** */
            [InstanceResource]
            public static IIgnite StaticGrid2;

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            public static IIgnite StaticPropGrid1
            {
                get { return _staticGrid1; }
                set { _staticGrid1 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            private static IIgnite StaticPropGrid2
            {
                get { return StaticGrid2; }
                set { StaticGrid2 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            public static void StaticMethod1(IIgnite grid)
            {
                _staticGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private static void StaticMethod2(IIgnite grid)
            {
                StaticGrid2 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            public Injectee()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public Injectee(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }

            /** */
            [InstanceResource]
            private readonly IIgnite _grid1 = null;

            /** */
            [InstanceResource]
            public IIgnite Grid2;

            /** */
            private IIgnite _mthdGrid1;

            /** */
            private IIgnite _mthdGrid2;

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            public IIgnite PropGrid1
            {
                get;
                set;
            }

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            private IIgnite PropGrid2
            {
                get;
                set;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            public void Method1(IIgnite grid)
            {
                _mthdGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private void Method2(IIgnite grid)
            {
                _mthdGrid2 = grid;
            }

            /// <summary>
            /// Check Ignite injections.
            /// </summary>
            protected void CheckInjection()
            {
                Assert.IsTrue(_staticGrid1 == null);
                Assert.IsTrue(StaticGrid2 == null);

                Assert.IsTrue(_grid1 != null);
                Assert.IsTrue(Grid2 == _grid1);

                Assert.IsTrue(PropGrid1 == _grid1);
                Assert.IsTrue(PropGrid2 == _grid1);

                Assert.IsTrue(_mthdGrid1 == _grid1);
                Assert.IsTrue(_mthdGrid2 == _grid1);
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }
        }

        /// <summary>
        ///
        /// </summary>
        [ComputeTaskNoResultCache]
        public class NoResultCacheTask : IComputeTask<int, int, int>
        {
            /** Sum. */
            private int _sum;

            /** <inheritDoc /> */
            public IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, int arg)
            {
                return subgrid.ToDictionary(x => (IComputeJob<int>) new NoResultCacheJob(), x => x);
            }

            /** <inheritDoc /> */
            public ComputeJobResultPolicy Result(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
            {
                Assert.IsTrue(rcvd != null);
                Assert.IsTrue(rcvd.Count == 0);

                _sum += res.Data();

                return ComputeJobResultPolicy.Wait;
            }

            /** <inheritDoc /> */
            public int Reduce(IList<IComputeJobResult<int>> results)
            {
                Assert.IsTrue(results != null);
                Assert.IsTrue(results.Count == 0);

                return _sum;
            }
        }

        /// <summary>
        ///
        /// </summary>
        [Serializable]
        public class NoResultCacheJob : IComputeJob<int>
        {
            /// <summary>
            ///
            /// </summary>
            public NoResultCacheJob()
            {
                // No-op.
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="info"></param>
            /// <param name="context"></param>
            public NoResultCacheJob(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }

            /** <inheritDoc /> */
            public int Execute()
            {
                return 1;
            }

            public void Cancel()
            {
                // No-op.
            }
        }
    }
}
