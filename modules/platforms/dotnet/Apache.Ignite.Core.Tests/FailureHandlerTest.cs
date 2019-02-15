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
            
            var proc = new IgniteProcess("-jvmClasspath=" + TestUtils.CreateTestClasspath(),
                "-configFileName=" + configFile);

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
