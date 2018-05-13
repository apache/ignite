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
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    public class FailureHandlerTest
    {
        /** Grid. */
        private IIgnite _grid;
        /// <summary>
        /// Set-up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();

            _grid = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration()));
            
            Assert.IsTrue(_grid.WaitTopology(1));
        }
        
        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);

            TestUtils.KillProcesses();
        }

        [Test]
        public void testStopNodeFailureHandler()
        {
            var proc = new IgniteProcess("-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-configFileName=config\\ignite-stophandler-dotnet-cfg.xml");

            Assert.IsTrue(proc.Alive);

            
            var ccfg = new CacheConfiguration("CacheWithFailedStore")
            {
                CacheStoreFactory = new FailedCacheStoreFactory(),
                ReadThrough = true
            };

            Assert.Throws<CacheException>(() => _grid.GetOrCreateCache<int, int>(ccfg));
                
            Assert.IsFalse(proc.Alive);
        }
        
        [Test]
        public void testStopNodeOrHaltFailureHandler()
        {
            var proc = new IgniteProcess("-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-configFileName=config\\ignite-halthandler-dotnet-cfg.xml");

            Assert.IsTrue(proc.Alive);

            
            var ccfg = new CacheConfiguration("CacheWithFailedStore")
            {
                CacheStoreFactory = new FailedCacheStoreFactory(),
                ReadThrough = true
            };

            Assert.Throws<CacheException>(() =>_grid.GetOrCreateCache<int, int>(ccfg));
            
            Assert.IsTrue(proc.Alive);
            
            Thread.Sleep(TimeSpan.Parse("0:0:7"));
            
            Assert.IsFalse(proc.Alive);

        }
         
        /// <summary>
        /// Test factory that fails grid.
        /// </summary>
        [Serializable]
        public class FailedCacheStoreFactory : IFactory<ICacheStore>
        {
            /// <summary>
            /// Creates an instance of the cache store. Throws an exception during creation. This cause grid to fail.
            /// </summary>
            public ICacheStore CreateInstance()
            {
                throw new Exception("FailedCacheStoreFactory.CreateInstance exception");
            }
        }

    }
}