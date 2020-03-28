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
    using Apache.Ignite.Core.Failure;
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests all three failure handlers: <see cref="StopNodeFailureHandler"/>, <see cref="StopNodeOrHaltFailureHandler"/>
    /// and <see cref="NoOpFailureHandler"/>. By default in test configuration we use <see cref="NoOpFailureHandler"/>.
    /// For others we should start node by <see cref="IgniteProcess"/>.
    /// </summary>
    public class FailureHandlerTest
    {
        /// <summary>
        /// Grid that uses <see cref="NoOpFailureHandler"/> and therefore can be started without
        /// <see cref="IgniteProcess"/>. 
        /// </summary>
        private IIgnite _grid;
        
        /** Part of failed CacheStore Exception message. */
        private const string CacheStoreExMessage = "Could not create .NET CacheStore";
        
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

        /// <summary>
        /// Tests <see cref="StopNodeFailureHandler"/>
        /// </summary>
        [Test]
        [Ignore("IGNITE-10364")]
        public void TestStopNodeFailureHandler()
        {
           TestFailureHandler(typeof(StopNodeFailureHandler));
        }
        
        /// <summary>
        /// Tests <see cref="StopNodeOrHaltFailureHandler"/>
        /// </summary>
        [Test]
        [Ignore("IGNITE-10364")]
        public void TestStopNodeOrHaltFailureHandler()
        {
            TestFailureHandler(typeof(StopNodeOrHaltFailureHandler));
        }

        private void TestFailureHandler(Type type)
        {
            var configFile = "config\\ignite-stophandler-dotnet-cfg.xml";
            
            if (type == typeof(StopNodeOrHaltFailureHandler))
            {
                configFile = "config\\ignite-halthandler-dotnet-cfg.xml";
            }
            
            var proc = new IgniteProcess("-configFileName=" + configFile);

            Assert.IsTrue(proc.Alive);

            
            var ccfg = new CacheConfiguration("CacheWithFailedStore")
            {
                CacheStoreFactory = new FailedCacheStoreFactory(),
                ReadThrough = true
            };

            var ex = Assert.Throws<CacheException>(() => _grid.GetOrCreateCache<int, int>(ccfg));
            
            Assert.IsTrue(ex.Message.Contains(CacheStoreExMessage));

            Thread.Sleep(TimeSpan.Parse("0:0:5"));
            
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
                throw new Exception("Failed cache store");
            }
        }
    }
}
