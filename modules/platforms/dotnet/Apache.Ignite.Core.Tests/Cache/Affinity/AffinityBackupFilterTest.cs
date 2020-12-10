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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="RendezvousAffinityFunction.AffinityBackupFilter"/>.
    /// </summary>
    public class AffinityBackupFilterTest
    {
        /// <summary>
        /// Tests that the presence of <see cref="RendezvousAffinityFunction.AffinityBackupFilter"/>
        /// affects backup nodes affinity assignment.
        /// </summary>
        [Test]
        public void TestBackupFilter()
        {
            for (int i = 0; i < 4; i++)
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    UserAttributes = new Dictionary<string, object>
                    {
                        {"DC", i < 2 ? 0 : 1},
                        {"IDX", i}
                    },
                    AutoGenerateIgniteInstanceName = true
                };

                Ignition.Start(cfg);
            }

            var ign = Ignition.GetAll().First();

            var cacheCfg = new CacheConfiguration
            {
                Name = "c",
                Backups = 1,
                AffinityFunction = new RendezvousAffinityFunction
                {
                    Partitions = 12,
                    // AffinityBackupFilter = new ClusterNodeAttributeAffinityBackupFilter
                    // {
                    //     AttributeNames = new[] {"DC"}
                    // }
                }
            };

            var cache = ign.CreateCache<int, int>(cacheCfg);
            cache[1] = 2;

            var aff = ign.GetAffinity(cache.Name);
            // var nodes = ign.GetCluster().GetNodes();
            //
            // foreach (var node in nodes)
            // {
            //     var parts = aff.GetBackupPartitions(node);
            //     Console.WriteLine(string.Join(", ", parts));
            // }

            var primaryAndBackup = aff.MapPartitionToPrimaryAndBackups(1);
            var primary = primaryAndBackup.First();
            var backup = primaryAndBackup.Last();

            Console.WriteLine("Primary: Node {0} in DC {1}", primary.Attributes["IDX"], primary.Attributes["DC"]);
            Console.WriteLine("Backup: Node {0} in DC {1}", backup.Attributes["IDX"], backup.Attributes["DC"]);
        }
    }
}
