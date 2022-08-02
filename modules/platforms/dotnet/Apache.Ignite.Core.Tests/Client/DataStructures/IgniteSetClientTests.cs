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
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.DataStructures;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IIgniteSetClient{T}"/>.
    /// Partition awareness tests: <see cref="PartitionAwarenessTest.IgniteSet_RequestIsRoutedToPrimaryNode"/>,
    /// <see cref="PartitionAwarenessTest.IgniteSetColocated_RequestIsRoutedToPrimaryNode"/>
    /// </summary>
    public class IgniteSetClientTests : ClientTestBase
    {
        [Test]
        public void TestCreateAddContainsRemove()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestCreateAddContainsRemove),
                GetCollectionConfiguration());

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
            Assert.IsFalse(set.IsClosed);
        }

        [Test]
        public void TestEnumeratorEmptySet()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestEnumeratorEmptySet),
                GetCollectionConfiguration());

            using var enumerator = set.GetEnumerator();

            Assert.Throws<InvalidOperationException>(() => _ = enumerator.Current);
            Assert.IsFalse(enumerator.MoveNext());
            Assert.Throws<InvalidOperationException>(() => _ = enumerator.Current);
        }

        [Test]
        public void TestEnumeratorMultiplePages()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestEnumeratorMultiplePages),
                GetCollectionConfiguration());

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
                GetCollectionConfiguration());

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
            var set = Client.GetIgniteSet<int>(nameof(TestClear), GetCollectionConfiguration());

            set.UnionWith(new[] { 1, 2 });
            set.Clear();

            Assert.AreEqual(0, set.Count);
            Assert.IsFalse(set.Contains(1));
            Assert.IsFalse(set.Contains(2));
        }

        [Test]
        public void TestExceptWith()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestExceptWith), GetCollectionConfiguration());

            set.UnionWith(new[] { 1, 2, 3 });
            set.ExceptWith(new[] { 1, 3, 5, 7 });
            set.ExceptWith(Array.Empty<int>());

            Assert.AreEqual(2, set.Single());
        }

        [Test]
        public void TestIntersectWith()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestIntersectWith), GetCollectionConfiguration());

            set.UnionWith(new[] { 1, 2, 3 });
            set.IntersectWith(new[] { 1, 3, 5 });

            CollectionAssert.AreEquivalent(new[] { 1, 3 }, set);
        }

        [Test]
        public void TestGetIgniteSetNonExistingReturnsNull()
        {
            Assert.IsNull(Client.GetIgniteSet<int>("foobar", null));
        }

        [Test]
        public void TestClose()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestClose), GetCollectionConfiguration());

            set.Add(1);
            set.Close();

            Assert.IsTrue(set.IsClosed);

            var ex = Assert.Throws<IgniteClientException>(() => set.Add(2));
            Assert.AreEqual("IgniteSet with name 'TestClose' does not exist.", ex.Message);
        }

        [Test]
        public void TestConfigurationPropagation()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestCopyTo()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestIsSupersetOf()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestIsProperSupersetOf()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestUnionWith()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestSetEquals()
        {
            Assert.Fail("TODO");
        }

        protected virtual CollectionClientConfiguration GetCollectionConfiguration() =>
            new CollectionClientConfiguration();
    }
}
