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
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="IDataStorageMetrics"/>.
    /// </summary>
    public class DataStorageMetricsTest
    {
        /** Temp dir for WAL. */
        private readonly string _tempDir = PathUtils.GetTempDirectoryName();

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

                Assert.GreaterOrEqual(metrics.LastCheckpointTotalPagesNumber, 1);
                Assert.AreEqual(0, metrics.LastCheckpointDataPagesNumber);
                Assert.GreaterOrEqual(metrics.LastCheckpointCopiedOnWritePagesNumber, 0);
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
