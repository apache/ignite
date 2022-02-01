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

namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="RendezvousAffinityFunction.AffinityBackupFilter"/>.
    /// </summary>
    public class AffinityBackupFilterTest
    {
        /** */
        private const string Rack = "Rack";

        /** */
        private const string NodeIdx = "Node_Idx";

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            for (int i = 0; i < 4; i++)
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    UserAttributes = new Dictionary<string, object>
                    {
                        {Rack, i < 2 ? 0 : 1},
                        {NodeIdx, i}
                    },
                    AutoGenerateIgniteInstanceName = true
                };

                Ignition.Start(cfg);
            }
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that the presence of <see cref="RendezvousAffinityFunction.AffinityBackupFilter"/>
        /// affects backup nodes affinity assignment.
        /// </summary>
        [Test]
        public void TestBackupFilterPlacesBackupsToDifferentRacks()
        {
            var ign = Ignition.GetAll().First();

            var cacheCfg1 = new CacheConfiguration("c1")
            {
                Backups = 1
            };

            var cacheCfg2 = new CacheConfiguration("c2")
            {
                Backups = 1,
                AffinityFunction = new RendezvousAffinityFunction
                {
                    Partitions = 12,
                    AffinityBackupFilter = new ClusterNodeAttributeAffinityBackupFilter
                    {
                        AttributeNames = new[] {Rack}
                    }
                }
            };

            var cache1 = ign.CreateCache<int, int>(cacheCfg1);
            var cache2 = ign.CreateCache<int, int>(cacheCfg2);

            var aff = ign.GetAffinity(cache1.Name);
            var aff2 = ign.GetAffinity(cache2.Name);

            var placement1 = GetPlacementString(aff.MapPartitionToPrimaryAndBackups(1));
            var placement2 = GetPlacementString(aff2.MapPartitionToPrimaryAndBackups(1));

            Assert.AreEqual(
                "Primary: Node 1 in Rack 0, Backup: Node 0 in Rack 0",
                placement1,
                "Without backup filter both backups are in the same rack.");

            Assert.AreEqual(
                "Primary: Node 1 in Rack 0, Backup: Node 2 in Rack 1",
                placement2,
                "With backup filter backups are in different racks.");
        }

        /// <summary>
        /// Gets the placement string.
        /// </summary>
        private static string GetPlacementString(IList<IClusterNode> primaryAndBackup)
        {
            var primary = primaryAndBackup.First();
            var backup = primaryAndBackup.Last();

            return string.Format(
                "Primary: Node {0} in Rack {1}, Backup: Node {2} in Rack {3}",
                primary.Attributes[NodeIdx],
                primary.Attributes[Rack],
                backup.Attributes[NodeIdx],
                backup.Attributes[Rack]);
        }
    }
}
