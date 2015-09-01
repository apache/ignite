/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Cache
{
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// Affinity key tests.
    /// </summary>
    public class GridCacheAffinityTest
    {
        /// <summary>
        ///
        /// </summary>
        [TestFixtureSetUp]
        public virtual void StartGrids()
        {
            GridTestUtils.KillProcesses();

            GridConfigurationEx cfg = new GridConfigurationEx();

            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = "config\\native-client-test-cache-affinity.xml";

            for (int i = 0; i < 3; i++)
            {
                cfg.GridName = "grid-" + i;

                GridFactory.Start(cfg);
            }
        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void StopGrids()
        {
            for (int i = 0; i < 3; i++)
                GridFactory.Stop("grid-" + i, true);
        }

        /// <summary>
        /// Test affinity key.
        /// </summary>
        [Test]
        public void TestAffinity()
        {
            IIgnite g = GridFactory.Grid("grid-0");

            ICacheAffinity aff = g.Affinity(null);

            IClusterNode node = aff.MapKeyToNode(new AffinityTestKey(0, 1));

            for (int i = 0; i < 10; i++)
                Assert.AreEqual(node.Id, aff.MapKeyToNode(new AffinityTestKey(i, 1)).Id);
        }

        /// <summary>
        /// Test affinity with portable flag.
        /// </summary>
        [Test]
        public void TestAffinityPortable()
        {
            IIgnite g = GridFactory.Grid("grid-0");

            ICacheAffinity aff = g.Affinity(null);  

            IPortableObject affKey = g.Portables().ToPortable<IPortableObject>(new AffinityTestKey(0, 1));

            IClusterNode node = aff.MapKeyToNode(affKey);

            for (int i = 0; i < 10; i++)
            {
                IPortableObject otherAffKey =
                    g.Portables().ToPortable<IPortableObject>(new AffinityTestKey(i, 1));

                Assert.AreEqual(node.Id, aff.MapKeyToNode(otherAffKey).Id);
            }
        }

        /// <summary>
        /// Affinity key.
        /// </summary>
        public class AffinityTestKey
        {
            /** ID. */
            private int id;

            /** Affinity key. */
            private int affKey;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="id">ID.</param>
            /// <param name="affKey">Affinity key.</param>
            public AffinityTestKey(int id, int affKey)
            {
                this.id = id;
                this.affKey = affKey;
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                AffinityTestKey other = obj as AffinityTestKey;

                return other != null && id == other.id;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return id;
            }
        }
    }
}
