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
namespace Apache.Ignite.Core.Tests.Binary
{
    using System.Collections.Generic;
    using NUnit.Framework;

    /// <summary>
    /// Tests storing collections in cache.
    /// TODO: Test ArrayList and Dictionary (non-generic).
    /// </summary>
    public class NestedCollectionHandlesCachePutGetTest
    {
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// TODO
        /// </summary>
        [Test]
        public void TestInnerList()
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, InnerList[]>(TestUtils.TestName);
            var inner = new List<object>();

            cache.Put(1, new[]
            {
                new InnerList {Inner = inner},
                new InnerList {Inner = inner}
            });

            var res = cache.Get(1);
            Assert.AreEqual(2, res.Length);
            Assert.AreNotSame(res[0], res[1]);
        }

        [Test]
        public void TestInnerObject()
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, InnerObject[]>("c");
            var inner = new object();

            cache.Put(1, new[]
            {
                new InnerObject {Inner = inner},
                new InnerObject {Inner = inner}
            });

            var res = cache.Get(1);
            Assert.AreEqual(2, res.Length);
            Assert.AreNotSame(res[0], res[1]);
        }

        [Test]
        public void TestInnerArray()
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, InnerArray[]>("c");
            var innerObj = new object();
            var inner = new[] {innerObj};

            cache.Put(1, new[]
            {
                new InnerArray {Inner = inner},
                new InnerArray {Inner = inner}
            });

            var res = cache.Get(1);
            Assert.AreEqual(2, res.Length);
            Assert.AreNotSame(res[0], res[1]);
        }

        [Test]
        public void TestInnerArrayReferenceLoop()
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, InnerArray[]>("c");
            var inner = new object[] {null};
            inner[0] = inner;

            cache.Put(1, new[]
            {
                new InnerArray {Inner = inner},
                new InnerArray {Inner = inner}
            });

            var res = cache.Get(1);
            Assert.AreEqual(2, res.Length);
            Assert.AreNotSame(res[0], res[1]);
            Assert.AreSame(res[0].Inner, res[0].Inner[0]);
        }

        [Test]
        [Ignore("TODO: StackOverflow in Java")]
        public void TestNestedArrayReferenceLoop()
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, object[][]>("c");
            var inner = new object[] {null};
            inner[0] = inner;

            cache.Put(1, new[]
            {
                new object[] {inner},
                new object[] {inner}
            });

            var res = cache.Get(1);
            Assert.AreEqual(2, res.Length);
            Assert.AreNotSame(res[0], res[1]);
        }

        private class InnerList
        {
            public IList<object> Inner { get; set; }
        }

        private class InnerObject
        {
            public object Inner { get; set; }
        }

        private class InnerArray
        {
            public object[] Inner { get; set; }
        }
    }
}
