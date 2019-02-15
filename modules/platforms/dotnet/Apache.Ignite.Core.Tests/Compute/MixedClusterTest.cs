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

namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests compute in a cluster with Java-only and .NET nodes.
    /// </summary>
    public class MixedClusterTest
    {
        /** */
        private IIgnite _ignite;
        
        /** */
        private string _javaNodeName;

        /** */
        private const string SpringConfig = @"Config\Compute\compute-grid1.xml";

        /** */
        private const string SpringConfig2 = @"Config\Compute\compute-grid2.xml";

        /** */
        private const string StartTask = "org.apache.ignite.platform.PlatformStartIgniteTask";

        /** */
        private const string StopTask = "org.apache.ignite.platform.PlatformStopIgniteTask";

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration()) {SpringConfigUrl = SpringConfig};

            _ignite = Ignition.Start(cfg);

            _javaNodeName = _ignite.GetCompute().ExecuteJavaTask<string>(StartTask, SpringConfig2);

            Assert.IsTrue(_ignite.WaitTopology(2));
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            _ignite.GetCompute().ExecuteJavaTask<object>(StopTask, _javaNodeName);
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the dot net task.
        /// </summary>
        [Test]
        public void TestDotNetTask()
        {
            var results = _ignite.GetCompute().Broadcast(new ComputeFunc());

            // There are two nodes, but only one can execute .NET jobs.
            Assert.AreEqual(new[] {int.MaxValue}, results.ToArray());
        }

        /// <summary>
        /// Tests the dot net task.
        /// </summary>
        [Test]
        public void TestJavaTask()
        {
            // Java task can execute on both nodes.
            var res = _ignite.GetCompute().ExecuteJavaTask<ICollection>(ComputeApiTest.BroadcastTask, null);

            Assert.AreEqual(2, res.Count);
        }

        /// <summary>
        /// Tests the cache invoke.
        /// </summary>
        [Test]
        public void TestCacheInvoke()
        {
            var cache = GetCache();

            var results = cache.InvokeAll(cache.Select(x => x.Key), new CacheProcessor(), 0);

            foreach (var res in results)
            {
                try
                {
                    Assert.AreEqual(0, res.Result);
                }
                catch (CacheEntryProcessorException ex)
                {
                    // At least some results should throw an error
                    Assert.IsTrue(ex.ToString().Contains("Platforms are not available"), "Unexpected error: " + ex);

                    return;
                }
            }

            Assert.Fail("InvokeAll unexpectedly succeeded in mixed-platform cluter.");
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<int, int> GetCache()
        {
            var cache = _ignite.GetOrCreateCache<int, int>("mixedCache");

            cache.PutAll(Enumerable.Range(1, 1000).ToDictionary(x => x, x => x));

            return cache;
        }

        /// <summary>
        /// Test func.
        /// </summary>
        [Serializable]
        private class ComputeFunc : IComputeFunc<int>
        {
            /** <inheritdoc /> */
            public int Invoke()
            {
                return int.MaxValue;
            }
        }

        /// <summary>
        /// Test processor.
        /// </summary>
        [Serializable]
        private class CacheProcessor : ICacheEntryProcessor<int, int, int, int>
        {
            /** <inheritdoc /> */
            public int Process(IMutableCacheEntry<int, int> entry, int arg)
            {
                return arg;
            }
        }
    }
}
