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

namespace Apache.Ignite.Core.Tests.AspNet
{
    using System;
    using System.Linq;
    using Apache.Ignite.AspNet.Impl;
    using Apache.Ignite.Core.Impl.Collections;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteSessionStateItemCollection"/>.
    /// </summary>
    public class IgniteSessionStateItemCollectionTest
    {
        /// <summary>
        /// Tests the empty collection.
        /// </summary>
        [Test]
        public void TestEmpty()
        {
            var col = new IgniteSessionStateItemCollection(new KeyValueDirtyTrackedCollection());

            // Check empty.
            Assert.IsFalse(col.Dirty);
            Assert.IsFalse(col.IsSynchronized);
            Assert.AreEqual(0, col.Count);
            Assert.IsNotNull(col.SyncRoot);
            Assert.IsEmpty(col);
            Assert.IsEmpty(col.OfType<string>().ToArray());
            Assert.IsEmpty(col.Keys);
            Assert.IsNotNull(col.SyncRoot);

            col.Clear();
            col.Remove("key");
            Assert.Throws<ArgumentOutOfRangeException>(() => col.RemoveAt(0));

            Assert.IsNull(col["key"]);
            Assert.Throws<ArgumentOutOfRangeException>(() => Assert.IsNull(col[0]));

            var keys = new[] {"1"};
            col.CopyTo(keys, 0);
            Assert.AreEqual(new[] {"1"}, keys);
        }

        /// <summary>
        /// Tests the modification.
        /// </summary>
        [Test]
        public void TestModification()
        {
            var innerCol = new KeyValueDirtyTrackedCollection();
            var col = new IgniteSessionStateItemCollection(innerCol);

            // Populate and check.
            col["key"] = "val";
            col["1"] = 1;

            Assert.AreEqual("val", col["key"]);
            Assert.AreEqual(1, col["1"]);

            Assert.AreEqual(2, col.Count);
            Assert.IsTrue(col.Dirty);

            CollectionAssert.AreEquivalent(new[] {"key", "1"}, col);
            CollectionAssert.AreEquivalent(new[] {"key", "1"}, col.Keys);

            // Modify using index.
            col[0] = "val1";
            col[1] = 2;

            Assert.AreEqual("val1", col["key"]);
            Assert.AreEqual(2, col["1"]);

            // Modify using key.
            col["1"] = 3;
            col["key"] = "val2";

            Assert.AreEqual("val2", col["key"]);
            Assert.AreEqual(3, col["1"]);

            // CopyTo.
            var keys = new string[5];
            col.CopyTo(keys, 2);
            Assert.AreEqual(new[] {null, null, "key", "1", null}, keys);

            // Remove.
            col["2"] = 2;
            col["3"] = 3;

            col.Remove("invalid");
            Assert.AreEqual(4, col.Count);

            col.Remove("1");

            Assert.AreEqual(new[] { "key", "2", "3" }, col.OfType<string>());
            Assert.AreEqual(null, col["1"]);

            Assert.AreEqual("val2", col["key"]);
            Assert.AreEqual("val2", col[0]);

            Assert.AreEqual(2, col["2"]);
            Assert.AreEqual(2, col[1]);

            Assert.AreEqual(3, col["3"]);
            Assert.AreEqual(3, col[2]);

            // RemoveAt.
            col.RemoveAt(0);
            Assert.AreEqual(new[] { "2", "3" }, col.OfType<string>());

            // Clear.
            Assert.AreEqual(2, col.Count);

            col.Clear();
            Assert.AreEqual(0, col.Count);

            // Set dirty.
            var col1 = new IgniteSessionStateItemCollection(innerCol) {Dirty = true};
            Assert.IsTrue(col1.Dirty);
        }
    }
}
