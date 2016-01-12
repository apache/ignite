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

namespace Apache.Ignite.Core.Tests
{
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Client reconnect tests.
    /// </summary>
    public class ReconnectTest
    {
        [Test]
        public void TestDisconnectedException()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\compute\\compute-grid1.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            var proc = StartServerProcess(cfg);

            Ignition.ClientMode = true;

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.IsTrue(ignite.WaitTopology(2, 30000));

                var cache = ignite.GetCache<int, int>(null);

                cache[1] = 1;

                proc.Suspend();

                Assert.Throws<CacheException>(() => cache.Get(1));

                // Reconnect
                proc.Resume();
                // TODO: Test future
                Assert.IsTrue(ignite.WaitTopology(2, 30000));

                Thread.Sleep(3000);

                cache = ignite.GetCache<int, int>(null); // TODO: remove this, reconnect should work properly

                Assert.AreEqual(1, cache[1]);
            }

            // TODO: Compute, services, etc..
        }

        private static IgniteProcess StartServerProcess(IgniteConfiguration cfg)
        {
            return new IgniteProcess(
                "-springConfigUrl=" + cfg.SpringConfigUrl, "-J-ea", "-J-Xcheck:jni", "-J-Xms512m", "-J-Xmx512m",
                "-J-DIGNITE_QUIET=false");
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            IgniteProcess.KillAll();
            Ignition.ClientMode = false;
        }
    }
}
