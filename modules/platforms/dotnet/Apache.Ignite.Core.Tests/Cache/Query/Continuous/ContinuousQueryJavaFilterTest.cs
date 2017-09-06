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
namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Interop;
    using NUnit.Framework;

    /// <summary>
    /// Tests query in a cluster with Java-only and .NET nodes.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
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
        private const string PlatformFilter = "org.apache.ignite.platform.PlatformCacheEntryEventFilter";

        /** */
        private const string PlatformFilterFactory = "org.apache.ignite.platform.PlatformCacheEntryEventFilterFactory";

        /** */
        private IIgnite _ignite;
        
        /** */
        private string _javaNodeName;

        /** */
        private static volatile Tuple<int, object> _lastEvent;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            // Main .NET node
            _ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = SpringConfig,
            });

            // Second .NET node
            Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = SpringConfig2,
                IgniteInstanceName = "dotNet2"
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
            var javaObj = new JavaObject(PlatformFilter)
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
                        "objField", new TestBinary(1, "2")

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
                            new TestBinary(1, "2")
                        }
                    },
                    {"arrayList", new ArrayList {"x"}},
                    {"hashTable", new Hashtable {{1, "2"}}}
                }
            };

            TestFilter(javaObj);
        }

        /// <summary>
        /// Tests the factory class.
        /// </summary>
        [Test]
        public void TestFactory()
        {
            var javaObj = new JavaObject(PlatformFilterFactory,
                new Dictionary<string, object> {{"startsWith", "valid"}});

            TestFilter(javaObj);
        }

        /// <summary>
        /// Tests the invalid class name
        /// </summary>
        [Test]
        public void TestInvalidClassName()
        {
            var javaObj = new JavaObject("blabla");

            var ex = Assert.Throws<IgniteException>(() => TestFilter(javaObj));

            Assert.IsTrue(ex.Message.StartsWith("Java object/factory class is not found"));
        }

        /// <summary>
        /// Tests the invalid class name
        /// </summary>
        [Test]
        public void TestInvalidProperty()
        {
            var javaObject = new JavaObject(PlatformFilter)
            {
                Properties = {{"invalidProp", "123"}}
            };

            var ex = Assert.Throws<IgniteException>(() => TestFilter(javaObject));

            Assert.IsTrue(ex.Message.StartsWith("Java object/factory class field is not found"));
        }

        /// <summary>
        /// Tests the specified filter.
        /// </summary>
        private void TestFilter(JavaObject obj)
        {
            TestFilter(obj, true);
            TestFilter(obj, false);
        }

        /// <summary>
        /// Tests the specified filter.
        /// </summary>
        private void TestFilter(JavaObject obj, bool local)
        {
            // Test with cache of strings
            var pred = obj.ToCacheEntryEventFilter<int, string>();

            TestFilter(pred, local, "validValue", "invalidValue");

            // Test with cache of binary objects
            var objPred = obj.ToCacheEntryEventFilter<int, TestBinary>();

            TestFilter(objPred, local, new TestBinary(1, "validValue"), new TestBinary(2, "invalidValue"));
        }

        /// <summary>
        /// Tests the specified filter.
        /// </summary>
        private void TestFilter<T>(ICacheEntryEventFilter<int, T> pred, bool local, T validVal, T invalidVal)
        {
            var cache = _ignite.GetOrCreateCache<int, T>("qry");
            cache.Clear();

            var qry = new ContinuousQuery<int, T>(new QueryListener<T>(), pred, local);
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
                    cache[key] = validVal;

                    TestUtils.WaitForCondition(() => _lastEvent != null, 2000);
                    Assert.IsNotNull(_lastEvent);
                    Assert.AreEqual(cache[key], _lastEvent.Item2);

                    _lastEvent = null;
                    cache[key] = invalidVal;

                    Thread.Sleep(2000);
                    Assert.IsNull(_lastEvent);
                }
            }
        }

        /// <summary>
        /// Test listener.
        /// </summary>
        private class QueryListener<T> : ICacheEntryEventListener<int, T>
        {
            /** <inheritdoc /> */
            public void OnEvent(IEnumerable<ICacheEntryEvent<int, T>> evts)
            {
                _lastEvent = evts.Select(e => Tuple.Create(e.Key, (object) e.Value)).FirstOrDefault();
            }
        }

        /// <summary>
        /// Test binary object.
        /// </summary>
        private class TestBinary
        {
            public TestBinary(int i, string s)
            {
                Int = i;
                String = s;
            }

            private int Int { get; set; }
            private string String { get; set; }

            private bool Equals(TestBinary other)
            {
                return Int == other.Int && string.Equals(String, other.String);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((TestBinary) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (Int*397) ^ (String != null ? String.GetHashCode() : 0);
                }
            }
        }
    }
}
