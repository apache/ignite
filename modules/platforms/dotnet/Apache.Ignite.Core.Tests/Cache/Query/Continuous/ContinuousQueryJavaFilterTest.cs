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

#pragma warning disable 618  // SpringConfigUrl
namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Interop;
    using NUnit.Framework;

    /// <summary>
    /// Tests query in a cluster with Java-only and .NET nodes.
    /// </summary>
    public class ContinuousQueryJavaFilterTest
    {
        /** */
        private const string SpringConfig = @"Config\Compute\compute-grid1.xml";

        /** */
        private const string SpringConfig2 = @"Config\Compute\compute-grid2.xml";

        /** */
        private const string StartTask = "org.apache.ignite.platform.PlatformStartIgniteTask";

        /** */
        private const string StopTask = "org.apache.ignite.platform.PlatformStopIgniteTask";

        /** */
        private IIgnite _ignite;
        
        /** */
        private string _javaNodeName;

        /** */
        private static volatile ICacheEntryEvent<int, string> _lastEvent;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            // Main .NET nodes
            _ignite =
                Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    SpringConfigUrl = SpringConfig
                });

            // Second .NET node
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = SpringConfig2,
                GridName = "dotNet2"
            });


            // Java-only node
            _javaNodeName = _ignite.GetCompute().ExecuteJavaTask<string>(StartTask, SpringConfig2);

            Assert.IsTrue(_ignite.WaitTopology(3));
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            _ignite.GetCompute().ExecuteJavaTask<object>(StopTask, _javaNodeName);
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the filter.
        /// </summary>
        [Test]
        public void TestFilter()
        {
            // TODO: Test all kinds of properties
            var javaObj = new JavaObject("org.apache.ignite.platform.PlatformCacheEntryEventFilter")
            {
                Properties = {{"startsWith", "valid"}}
            };

            var filter = javaObj.ToCacheEntryEventFilter<int, string>();

            TestFilter(filter);
        }

        /// <summary>
        /// Tests the factory class.
        /// </summary>
        [Test]
        public void TestFactory()
        {
            var javaObj = new JavaObject("org.apache.ignite.platform.PlatformCacheEntryEventFilterFactory",
                new Dictionary<string, object> {{"startsWith", "valid"}});

            var filter = javaObj.ToCacheEntryEventFilter<int, string>();

            TestFilter(filter);
        }

        /// <summary>
        /// Tests the invalid class name
        /// </summary>
        [Test]
        public void TestInvalidClassName()
        {
            var filter = new JavaObject("blabla").ToCacheEntryEventFilter<int, string>();

            TestFilter(filter);
        }

        /// <summary>
        /// Tests the invalid class name
        /// </summary>
        [Test]
        public void TestInvalidProperty()
        {
            var javaObject = new JavaObject("org.apache.ignite.platform.PlatformCacheEntryEventFilter")
            {
                Properties = {{"invalidProp", "123"}}
            };

            var filter = javaObject.ToCacheEntryEventFilter<int, string>();

            TestFilter(filter);
        }

        /// <summary>
        /// Tests the specified filter.
        /// </summary>
        private void TestFilter(ICacheEntryEventFilter<int, string> pred)
        {
            var cache = _ignite.GetOrCreateCache<int, string>("qry");
            var qry = new ContinuousQuery<int, string>(new QueryListener(), pred);

            using (cache.QueryContinuous(qry))
            {
                // Run on many keys to test all nodes
                for (var i = 0; i < 200; i++)
                {
                    _lastEvent = null;
                    cache[i] = "validValue";
                    // ReSharper disable once PossibleNullReferenceException
                    Assert.AreEqual(cache[i], _lastEvent.Value);

                    _lastEvent = null;
                    cache[i] = "invalidValue";
                    Assert.IsNull(_lastEvent);
                }
            }
        }

        /// <summary>
        /// Test listener.
        /// </summary>
        private class QueryListener : ICacheEntryEventListener<int, string>
        {
            /** <inheritdoc /> */
            public void OnEvent(IEnumerable<ICacheEntryEvent<int, string>> evts)
            {
                _lastEvent = evts.FirstOrDefault();
            }
        }
    }
}
