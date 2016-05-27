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

// ReSharper disable UnusedAutoPropertyAccessor.Local
#pragma warning disable 618  // SpringConfigUrl
namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
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
            IList<String> jvmOpts = TestUtils.TestJavaOptions();

            _ignite = Ignition.Start(new IgniteConfiguration
            {
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = jvmOpts,
                SpringConfigUrl = SpringConfig,
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof(TestBinary)) 
                    }
                }
            });

            // Second .NET node
            Ignition.Start(new IgniteConfigurationEx
            {
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = jvmOpts,
                SpringConfigUrl = SpringConfig2,
                GridName = "dotNet2"
            });

            // Java-only node
            _javaNodeName = _ignite.GetCompute().ExecuteJavaTask<string>(StartTask, SpringConfig2);

            Assert.IsTrue(_ignite.WaitTopology(3, 5000));
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
            var javaObj = new JavaObject("org.apache.ignite.platform.PlatformCacheEntryEventFilter")
            {
                Properties =
                {
                    {"startsWith", "valid"},
                    {"charField", 'a'},
                    {"byteField", (byte) 1},
                    {"sbyteField", (sbyte) 2},
                    {"shortField", (short) 3},
                    {"ushortField", (ushort) 4},
                    {"intField", 5},
                    {"uintField", (uint) 6},
                    {"longField", (long) 7},
                    {"ulongField", (ulong) 8},
                    {"floatField", (float) 9.99},
                    {"doubleField", 10.123},
                    {"decimalField", (decimal) 11.245},
                    {"boolField", true},
                    {"guidField", Guid.Parse("1c579241-509d-47c6-a1a0-87462ae31e59")},
                    {
                        "objField", new TestBinary
                        {
                            Int = 1,
                            String = "2"
                        }
                    },
                    {"charArr", new[] {'a'}},
                    {"byteArr", new[] {(byte) 1}},
                    {"sbyteArr", new[] {(sbyte) 2}},
                    {"shortArr", new[] {(short) 3}},
                    {"ushortArr", new[] {(ushort) 4}},
                    {"intArr", new[] {5}},
                    {"uintArr", new[] {(uint) 6}},
                    {"longArr", new[] {(long) 7}},
                    {"ulongArr", new[] {(ulong) 8}},
                    {"floatArr", new[] {(float) 9.99}},
                    {"doubleArr", new[] {10.123}},
                    {"boolArr", new[] {true}},
                    {
                        "objArr", new object[]
                        {
                            new TestBinary
                            {
                                Int = 1,
                                String = "2"
                            }
                        }
                    },
                    {"arrayList", new ArrayList {"x"}},
                    {"hashTable", new Hashtable {{1, "2"}}}
                }
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

            var ex = Assert.Throws<IgniteException>(() => TestFilter(filter));

            Assert.IsTrue(ex.Message.StartsWith("Java object/factory class is not found"));
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

            var ex = Assert.Throws<IgniteException>(() => TestFilter(filter));

            Assert.IsTrue(ex.Message.StartsWith("Java object/factory class field is not found"));
        }

        /// <summary>
        /// Tests the specified filter.
        /// </summary>
        private void TestFilter(ICacheEntryEventFilter<int, string> pred)
        {
            TestFilter(pred, false);
            TestFilter(pred, true);
        }

        /// <summary>
        /// Tests the specified filter.
        /// </summary>
        private void TestFilter(ICacheEntryEventFilter<int, string> pred, bool local)
        {
            var cache = _ignite.GetOrCreateCache<int, string>("qry");
            var qry = new ContinuousQuery<int, string>(new QueryListener(), pred, local);
            var aff = _ignite.GetAffinity("qry");
            var localNode = _ignite.GetCluster().GetLocalNode();

            // Get one key per node
            var keyMap = aff.MapKeysToNodes(Enumerable.Range(1, 100));
            Assert.AreEqual(3, keyMap.Count);
            var keys = local
                ? keyMap[localNode].Take(1)
                : keyMap.Select(x => x.Value.First());

            using (cache.QueryContinuous(qry))
            {
                // Run on many keys to test all nodes
                foreach (var key in keys)
                {
                    _lastEvent = null;
                    cache[key] = "validValue";

                    TestUtils.WaitForCondition(() => _lastEvent != null, 2000);
                    Assert.IsNotNull(_lastEvent);
                    Assert.AreEqual(cache[key], _lastEvent.Value);

                    _lastEvent = null;
                    cache[key] = "invalidValue";

                    Thread.Sleep(2000);
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

        /// <summary>
        /// Test binary object.
        /// </summary>
        private class TestBinary
        {
            public int Int { get; set; }
            public string String { get; set; }
        }
    }
}
