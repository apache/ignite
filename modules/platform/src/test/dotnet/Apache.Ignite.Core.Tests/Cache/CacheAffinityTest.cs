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
    public class CacheAffinityTest
    {
        /// <summary>
        ///
        /// </summary>
        [TestFixtureSetUp]
        public virtual void StartGrids()
        {
            TestUtils.KillProcesses();

            IgniteConfigurationEx cfg = new IgniteConfigurationEx();

            cfg.JvmClasspath = TestUtils.CreateTestClasspath();
            cfg.JvmOptions = TestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = "config\\native-client-test-cache-affinity.xml";

            for (int i = 0; i < 3; i++)
            {
                cfg.GridName = "grid-" + i;

                Ignition.Start(cfg);
            }
        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void StopGrids()
        {
            for (int i = 0; i < 3; i++)
                Ignition.Stop("grid-" + i, true);
        }

        /// <summary>
        /// Test affinity key.
        /// </summary>
        [Test]
        public void TestAffinity()
        {
            IIgnite g = Ignition.GetIgnite("grid-0");

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
            IIgnite g = Ignition.GetIgnite("grid-0");

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
            private int _id;

            /** Affinity key. */
            private int _affKey;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="id">ID.</param>
            /// <param name="affKey">Affinity key.</param>
            public AffinityTestKey(int id, int affKey)
            {
                _id = id;
                _affKey = affKey;
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                AffinityTestKey other = obj as AffinityTestKey;

                return other != null && _id == other._id;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return _id;
            }
        }
    }
}
