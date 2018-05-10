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
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests Ignite behavior in multi-AppDomain scenario.
    /// Such a scenario occurs within IIS, for example, or within application plugins.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class MultiAppDomainTest
    {
        /** */
        public const string CacheName = "cache";

        /** */
        private readonly List<AppDomain> _domains = new List<AppDomain>();

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);

            UnloadDomains();
        }

        /// <summary>
        /// Tests the IIS behavior:
        /// when application is restarted, new AppDomain is started while old one is still running.
        /// </summary>
        [Test]
        public void TestIisBehavior()
        {
            var ignite = Ignition.Start(GetConfig());
            
            var cache = ignite.CreateCache<int, int>(new CacheConfiguration
            {
                Name =  CacheName,
                CacheMode = CacheMode.Replicated  // Use Replicated to avoid data loss due to node stop.
            });

            cache[1] = 1;

            // Check same domain.
            new DomainRunner().RunTest();

            var type = typeof(DomainRunner);
            Assert.IsNotNull(type.FullName);

            // Start and stop domains.
            for (var i = 0; i < 10; i++)
            {
                var domain = CreateDomain(i);

                var runner = (DomainRunner) domain.CreateInstanceAndUnwrap(type.Assembly.FullName, type.FullName);
                runner.RunTest();

                // Verify node start.
                var expectedNodeCount = Math.Min(i + 3, 7);
                Assert.AreEqual(expectedNodeCount, ignite.GetCluster().GetNodes().Count);

                // Current AppDomain does not see other instances.
                Assert.AreEqual(2, Ignition.GetAll().Count);

                if (i > 3)
                {
                    var oldDomain = _domains[i - 3];
                    _domains[i - 3] = null;

                    AppDomain.Unload(oldDomain);

                    // Verify node exit.
                    TestUtils.WaitForCondition(
                        () => ignite.GetCluster().GetNodes().Count == expectedNodeCount - 1, 5000);
                }
            }

            UnloadDomains();

            // Verify node exit: only two nodes from current domain should be there.
            TestUtils.WaitForCondition(() => ignite.GetCluster().GetNodes().Count == 2, 5000);
        }

        /// <summary>
        /// Creates the domain.
        /// </summary>
        private AppDomain CreateDomain(int i)
        {
            var domain = AppDomain.CreateDomain("TestIisBehavior-" + i, null, new AppDomainSetup
            {
                ApplicationBase = AppDomain.CurrentDomain.SetupInformation.ApplicationBase,
                ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile,
                ApplicationName = AppDomain.CurrentDomain.SetupInformation.ApplicationName,
                LoaderOptimization = LoaderOptimization.MultiDomainHost
            });

            _domains.Add(domain);

            return domain;
        }

        /// <summary>
        /// Unloads the domains.
        /// </summary>
        private void UnloadDomains()
        {
            foreach (var appDomain in _domains)
            {
                if (appDomain != null)
                {
                    AppDomain.Unload(appDomain);
                }
            }

            _domains.Clear();
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private static IgniteConfiguration GetConfig()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                AutoGenerateIgniteInstanceName = true
            };
        }

        /// <summary>
        /// Class to instantiate in another domain.
        /// </summary>
        private class DomainRunner : MarshalByRefObject
        {
            /// <summary>
            /// Runs the test.
            /// </summary>
            public void RunTest()
            {
                var cfg = GetConfig();

                // No need to stop: this will happen on domain unload.
                var ignite = Ignition.Start(cfg);
                
                var cache = ignite.GetCache<int, int>(CacheName);
                Assert.AreEqual(1, cache[1]);
            }
        }
    }
}
