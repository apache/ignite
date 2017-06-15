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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests the persistent store.
    /// </summary>
    public class PersistentStoreTest
    {
        /// <summary>
        /// Tests the grid activation with persistence (inactive by default).
        /// </summary>
        [Test]
        public void TestGridActivationWithPersistence()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                PersistentStoreConfiguration = new PersistentStoreConfiguration()
            };

            using (var ignite = Ignition.Start(cfg))
            {
                CheckIsActive(ignite, false);

                ignite.SetActive(true);
                CheckIsActive(ignite, true);

                ignite.SetActive(false);
                CheckIsActive(ignite, false);
            }
        }
        
        /// <summary>
        /// Tests the grid activation without persistence (active by default).
        /// </summary>
        [Test]
        public void TestGridActivationNoPersistence()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                CheckIsActive(ignite, true);

                ignite.SetActive(false);
                CheckIsActive(ignite, false);

                ignite.SetActive(true);
                CheckIsActive(ignite, true);
            }
        }

        /// <summary>
        /// Checks active state.
        /// </summary>
        private static void CheckIsActive(IIgnite ignite, bool isActive)
        {
            Assert.AreEqual(isActive, ignite.IsActive());

            if (isActive)
            {
                var cache = ignite.GetOrCreateCache<int, int>("default");
                cache[1] = 1;
                Assert.AreEqual(1, cache[1]);
            }
            else
            {
                var ex = Assert.Throws<IgniteException>(() => ignite.GetOrCreateCache<int, int>("default"));
                Assert.AreEqual("can not perform operation, because cluster inactive", ex.Message);
            }
        }
    }
}
