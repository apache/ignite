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
    using System.Reflection;
    using Apache.Ignite.Core.Cache.Configuration;
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
            var cfg = new CollectionClientConfiguration
            {
                Backups = 7,
                Colocated = false,
                AtomicityMode = CacheAtomicityMode.Transactional,
                CacheMode = CacheMode.Partitioned,
                GroupName = "g-" + nameof(TestConfigurationPropagation)
            };

            var set = Client.GetIgniteSet<int>(nameof(TestConfigurationPropagation), cfg);

            const string cacheName = "datastructures_TRANSACTIONAL_PARTITIONED_7@" +
                                     "g-TestConfigurationPropagation#SET_TestConfigurationPropagation";

            var cache = Client.GetCache<int, int>(cacheName);

            Assert.AreEqual(
                set.GetType().GetField("_cacheId", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(set),
                cache.GetType().GetField("_id", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(cache));
        }

        [Test]
        public void TestCopyTo()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestCopyTo), GetCollectionConfiguration());
            set.UnionWith(new[] { 1, 2, 3 });

            var target1 = new int[3];
            set.CopyTo(target1, 0);

            var target2 = new int[4];
            set.CopyTo(target2, 1);

            CollectionAssert.AreEquivalent(set, target1);
            CollectionAssert.AreEquivalent(set, target2.Skip(1));
        }

        [Test]
        public void TestCopyToInvalidArguments()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestCopyToInvalidArguments), GetCollectionConfiguration());
            set.UnionWith(new[] { 1, 2, 3 });

            var target1 = new int[3];

            var ex1 = Assert.Throws<ArgumentOutOfRangeException>(() => set.CopyTo(target1, -1));
            StringAssert.StartsWith("Non-negative number required.", ex1.Message);

            var ex2 = Assert.Throws<ArgumentException>(() => set.CopyTo(target1, 1));
            StringAssert.StartsWith("Destination array is not long enough to copy all the items in the collection",
                ex2.Message);

            var ex3 = Assert.Throws<ArgumentException>(() => set.CopyTo(target1, 3));
            StringAssert.StartsWith("Destination array is not long enough to copy all the items in the collection",
                ex3.Message);
        }

        [Test]
        public void TestUserObjects()
        {
            var set = Client.GetIgniteSet<CustomClass>(nameof(TestUserObjects), GetCollectionConfiguration());

            set.Add(new CustomClass { Id = 1, Name = "A" });
            set.IntersectWith(new[] { new CustomClass { Id = 1, Name = "A" }, new CustomClass { Id = 2, Name = "B" } });

            Assert.IsTrue(set.Contains(new CustomClass { Id = 1, Name = "A" }));
            Assert.IsFalse(set.Contains(new CustomClass { Id = 1, Name = "a" }));

            var res = set.Single();

            Assert.AreEqual(1, res.Id);
            Assert.AreEqual("A", res.Name);

            set.Clear();
            Assert.AreEqual(0, set.Count);
        }

        [Test]
        public void TestIsSupersetOf()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestIsSupersetOf), GetCollectionConfiguration());
            set.UnionWith(new[] { 1, 2, 3 });

            Assert.IsTrue(set.IsSupersetOf(new[] { 3, 2, 1 }));
            Assert.IsTrue(set.IsSupersetOf(new[] { 3, 2 }));
            Assert.IsFalse(set.IsSupersetOf(new[] { 3, 0 }));
        }

        [Test]
        public void TestIsProperSupersetOf()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestIsProperSupersetOf), GetCollectionConfiguration());
            set.UnionWith(new[] { 1, 2, 3 });

            Assert.IsFalse(set.IsProperSupersetOf(new[] { 3, 2, 1 }));
            Assert.IsTrue(set.IsProperSupersetOf(new[] { 3, 2 }));
            Assert.IsTrue(set.IsProperSupersetOf(new[] { 1 }));
            Assert.IsFalse(set.IsProperSupersetOf(new[] { 3, 0 }));
        }

        [Test]
        public void TestUnionWith()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestUnionWith), GetCollectionConfiguration());
            set.UnionWith(new[] { 1 });
            set.UnionWith(new[] { 1, 2 });
            set.UnionWith(new[] { 0, 3 });

            CollectionAssert.AreEquivalent(new[] { 0, 1, 2, 3 }, set);
        }

        [Test]
        public void TestSetEquals()
        {
            var set = Client.GetIgniteSet<int>(nameof(TestSetEquals), GetCollectionConfiguration());
            set.UnionWith(new[] { 1, 2, 3 });

            Assert.IsTrue(set.SetEquals(new[] { 1, 2, 3 }));
            Assert.IsTrue(set.SetEquals(new[] { 3, 1, 2 }));
            Assert.IsFalse(set.SetEquals(new[] { 1, 2 }));
            Assert.IsFalse(set.SetEquals(new[] { 1, 2, 4 }));
            Assert.IsFalse(set.SetEquals(new[] { 1, 2, 3, 4 }));
        }

        protected virtual CollectionClientConfiguration GetCollectionConfiguration() =>
            new CollectionClientConfiguration();

        private class CustomClass
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }
    }
}
