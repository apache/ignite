/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

// ReSharper disable UnusedMember.Local
// ReSharper disable UnusedMember.Global
// ReSharper disable UnusedParameter.Local
// ReSharper disable UnusedAutoPropertyAccessor.Local
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
    public class GridResourceTaskTest : GridAbstractTaskTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public GridResourceTaskTest() : base(false) { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fork">Fork flag.</param>
        protected GridResourceTaskTest(bool fork) : base(fork) { }

        /// <summary>
        /// Test Grid injection into the task.
        /// </summary>
        [Test]
        public void TestTaskGridInjection()
        {
            int res = grid1.Compute().Execute(new InjectionTask(), 0);

            Assert.AreEqual(grid1.Cluster.Nodes().Count, res);
        }

        /// <summary>
        /// Test Grid injection into the closure.
        /// </summary>
        [Test]
        public void TestClosureGridInjection()
        {
            var res = grid1.Compute().Broadcast(new InjectionClosure(), 1);

            Assert.AreEqual(grid1.Cluster.Nodes().Count, res.Sum());
        }

        /// <summary>
        /// Test Grid injection into reducer.
        /// </summary>
        [Test]
        public void TestReducerGridInjection()
        {
            int res = grid1.Compute().Apply(new InjectionClosure(), new List<int> { 1, 1, 1 }, new InjectionReducer());

            Assert.AreEqual(grid1.Cluster.Nodes().Count, res);
        }

        /// <summary>
        /// Test no-result-cache attribute.
        /// </summary>
        [Test]
        public void TestNoResultCache()
        {
            int res = grid1.Compute().Execute(new NoResultCacheTask(), 0);

            Assert.AreEqual(grid1.Cluster.Nodes().Count, res);
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
                return ComputeJobResultPolicy.WAIT;
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
            private static IIgnite staticGrid1;

            /** */
            [InstanceResource]
            public static IIgnite staticGrid2;

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            public static IIgnite StaticPropGrid1
            {
                get { return staticGrid1; }
                set { staticGrid1 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            private static IIgnite StaticPropGrid2
            {
                get { return staticGrid2; }
                set { staticGrid2 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            public static void StaticMethod1(IIgnite grid)
            {
                staticGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private static void StaticMethod2(IIgnite grid)
            {
                staticGrid2 = grid;
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
            private readonly IIgnite grid1 = null;

            /** */
            [InstanceResource]
            public IIgnite grid2;

            /** */
            private IIgnite mthdGrid1;

            /** */
            private IIgnite mthdGrid2;

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
                mthdGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private void Method2(IIgnite grid)
            {
                mthdGrid2 = grid;
            }

            /// <summary>
            /// Check Grid injections.
            /// </summary>
            protected void CheckInjection()
            {
                Assert.IsTrue(staticGrid1 == null);
                Assert.IsTrue(staticGrid2 == null);

                Assert.IsTrue(grid1 != null);
                Assert.IsTrue(grid2 == grid1);

                Assert.IsTrue(PropGrid1 == grid1);
                Assert.IsTrue(PropGrid2 == grid1);

                Assert.IsTrue(mthdGrid1 == grid1);
                Assert.IsTrue(mthdGrid2 == grid1);
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
            private readonly ICollection<int> ress = new List<int>();

            /** <inheritDoc /> */
            public bool Collect(int res)
            {
                CheckInjection();

                lock (ress)
                {
                    ress.Add(res);
                }

                return true;
            }

            /** <inheritDoc /> */
            public int Reduce()
            {
                CheckInjection();

                lock (ress)
                {
                    return ress.Sum();
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
            private static IIgnite staticGrid1;

            /** */
            [InstanceResource]
            public static IIgnite staticGrid2;

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            public static IIgnite StaticPropGrid1
            {
                get { return staticGrid1; }
                set { staticGrid1 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            [InstanceResource]
            private static IIgnite StaticPropGrid2
            {
                get { return staticGrid2; }
                set { staticGrid2 = value; }
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            public static void StaticMethod1(IIgnite grid)
            {
                staticGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private static void StaticMethod2(IIgnite grid)
            {
                staticGrid2 = grid;
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
            private readonly IIgnite grid1 = null;

            /** */
            [InstanceResource]
            public IIgnite grid2;

            /** */
            private IIgnite mthdGrid1;

            /** */
            private IIgnite mthdGrid2;

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
                mthdGrid1 = grid;
            }

            /// <summary>
            ///
            /// </summary>
            /// <param name="grid"></param>
            [InstanceResource]
            private void Method2(IIgnite grid)
            {
                mthdGrid2 = grid;
            }

            /// <summary>
            /// Check Grid injections.
            /// </summary>
            protected void CheckInjection()
            {
                Assert.IsTrue(staticGrid1 == null);
                Assert.IsTrue(staticGrid2 == null);

                Assert.IsTrue(grid1 != null);
                Assert.IsTrue(grid2 == grid1);

                Assert.IsTrue(PropGrid1 == grid1);
                Assert.IsTrue(PropGrid2 == grid1);

                Assert.IsTrue(mthdGrid1 == grid1);
                Assert.IsTrue(mthdGrid2 == grid1);
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
            private int sum;

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

                sum += res.Data();

                return ComputeJobResultPolicy.WAIT;
            }

            /** <inheritDoc /> */
            public int Reduce(IList<IComputeJobResult<int>> results)
            {
                Assert.IsTrue(results != null);
                Assert.IsTrue(results.Count == 0);

                return sum;
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
