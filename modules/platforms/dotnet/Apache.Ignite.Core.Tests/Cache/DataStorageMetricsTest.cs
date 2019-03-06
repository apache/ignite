/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="IDataStorageMetrics"/>.
    /// </summary>
    public class DataStorageMetricsTest
    {
        /** Temp dir for WAL. */
        private readonly string _tempDir = IgniteUtils.GetTempDirectoryName();

        /// <summary>
        /// Tests the data storage metrics.
        /// </summary>
        [Test]
        public void TestDataStorageMetrics()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    CheckpointFrequency = TimeSpan.FromSeconds(5),
                    MetricsEnabled = true,
                    WalMode = WalMode.LogOnly,
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        PersistenceEnabled = true,
                        Name = "foobar"
                    }
                },
                WorkDirectory = _tempDir
            };

            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCluster().SetActive(true);

                var cache = ignite.CreateCache<int, object>("c");

                cache.PutAll(Enumerable.Range(1, 10)
                    .ToDictionary(x => x, x => (object) new {Name = x.ToString(), Id = x}));

                // Wait for checkpoint and metrics update and verify.
                IDataStorageMetrics metrics = null;

                Assert.IsTrue(TestUtils.WaitForCondition(() =>
                {
                    // ReSharper disable once AccessToDisposedClosure
                    metrics = ignite.GetDataStorageMetrics();

                    return metrics.LastCheckpointTotalPagesNumber > 0;
                }, 10000));

                Assert.IsNotNull(metrics);

                Assert.AreEqual(0, metrics.WalArchiveSegments);
                Assert.Greater(metrics.WalFsyncTimeAverage, 0);

                Assert.GreaterOrEqual(metrics.LastCheckpointTotalPagesNumber, 26);
                Assert.AreEqual(0, metrics.LastCheckpointDataPagesNumber);
                Assert.AreEqual(0, metrics.LastCheckpointCopiedOnWritePagesNumber);
                Assert.Greater(TimeSpan.FromSeconds(1), metrics.LastCheckpointLockWaitDuration);

                Assert.Greater(metrics.LastCheckpointPagesWriteDuration, TimeSpan.Zero);
                Assert.Greater(metrics.LastCheckpointMarkDuration, TimeSpan.Zero);
                Assert.Greater(metrics.LastCheckpointDuration, TimeSpan.Zero);
                Assert.Greater(metrics.LastCheckpointFsyncDuration, TimeSpan.Zero);

                Assert.Greater(metrics.LastCheckpointDuration, metrics.LastCheckpointMarkDuration);
                Assert.Greater(metrics.LastCheckpointDuration, metrics.LastCheckpointPagesWriteDuration);
                Assert.Greater(metrics.LastCheckpointDuration, metrics.LastCheckpointFsyncDuration);
            }
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Directory.Delete(_tempDir, true);
        }
    }
}
