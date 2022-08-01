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

namespace Apache.Ignite.Core.Tests.Client.DataStructures
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Client.DataStructures;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IIgniteSetClient{T}"/>.
    /// </summary>
    public class IgniteSetClientTests : ClientTestBase
    {
        [Test]
        public void TestCreateAddContainsRemove()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestCreateAddContainsRemove),
                new CollectionClientConfiguration());

            set.Add(1);
            set.Add(1);

            set.Add(2);
            set.Add(2);

            set.Remove(1);

            Assert.AreEqual(nameof(TestCreateAddContainsRemove), set.Name);
            Assert.IsTrue(set.Contains(2));
            Assert.IsFalse(set.Contains(1));
            Assert.AreEqual(1, set.Count);
            Assert.AreEqual(2, set.Single());
        }

        [Test]
        public void TestEnumeratorEmptySet()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestEnumeratorEmptySet),
                new CollectionClientConfiguration());

            using var enumerator = set.GetEnumerator();

            Assert.Throws<InvalidOperationException>(() => _ = enumerator.Current);
            Assert.IsFalse(enumerator.MoveNext());
            Assert.Throws<InvalidOperationException>(() => _ = enumerator.Current);
        }

        [Test]
        public void TestEnumeratorMultiplePages()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestEnumeratorMultiplePages),
                new CollectionClientConfiguration());

            set.PageSize = 2;

            var items = Enumerable.Range(1, 10).ToList();
            items.ForEach(x => set.Add(x));

            var res = set.ToList();

            CollectionAssert.AreEquivalent(items, res);
        }

        [Test]
        public void TestEnumeratorRemoveAllWhileEnumerating()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestEnumeratorRemoveAllWhileEnumerating),
                new CollectionClientConfiguration());

            set.PageSize = 2;

            var items = Enumerable.Range(1, 10).ToList();
            items.ForEach(x => set.Add(x));

            using var enumerator = set.GetEnumerator();

            items.ForEach(x => set.Remove(x));

            var res = new List<int>();

            while (enumerator.MoveNext())
            {
                res.Add(enumerator.Current);
            }

            // 2 from current page, 1 from current item cached on server.
            Assert.AreEqual(3, res.Count);
        }

        [Test]
        public void TestClear()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestClear),
                new CollectionClientConfiguration());

            set.Add(1);
            set.Add(2);

            set.Clear();

            Assert.AreEqual(0, set.Count);
            Assert.IsFalse(set.Contains(1));
            Assert.IsFalse(set.Contains(2));
        }

        [Test]
        public void TestGetIgniteSetNonExistingReturnsNull()
        {
            Assert.IsNull(Client.GetIgniteSet<int>("foobar", null));
        }
    }
}
