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

namespace Apache.Ignite.AspNet.Tests
{
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.AspNet.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
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
            var col1 = new IgniteSessionStateItemCollection();
            var col2 = SerializeDeserialize(col1);

            foreach (var col in new[] { col1, col2 })
            {
                Assert.IsFalse(col.Dirty);
                Assert.IsFalse(col.IsSynchronized);
                Assert.AreEqual(0, col.Count);
                Assert.IsNotNull(col.SyncRoot);
                Assert.IsEmpty(col);
                Assert.IsEmpty(col.OfType<string>().ToArray());
                Assert.IsEmpty(col.Keys);
                Assert.IsNotNull(col.SyncRoot);

                Assert.IsNull(col["key"]);
                Assert.Throws<ArgumentOutOfRangeException>(() => col[0] = "x");
                Assert.Throws<ArgumentOutOfRangeException>(() => Assert.AreEqual(0, col[0]));
                Assert.Throws<ArgumentOutOfRangeException>(() => col.RemoveAt(0));

                col.Clear();
                col.Remove("test");

                Assert.AreEqual(0, col.Count);

                col.Dirty = true;
                Assert.IsTrue(col.Dirty);
            }
        }

        /// <summary>
        /// Tests the modification.
        /// </summary>
        [Test]
        public void TestModification()
        {
            var col = new IgniteSessionStateItemCollection();

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
            var col1 = new IgniteSessionStateItemCollection {Dirty = true};
            Assert.IsTrue(col1.Dirty);
        }

        /// <summary>
        /// Tests dirty tracking.
        /// </summary>
        [Test]
        public void TestApplyChanges()
        {
            Func<IgniteSessionStateItemCollection> getCol = () =>
            {
                var res = new IgniteSessionStateItemCollection();

                res["1"] = 1;
                res["2"] = 2;
                res["3"] = 3;

                return res;
            };

            var col = getCol();

            var col0 = SerializeDeserialize(col);

            Assert.AreEqual(3, col0.Count);

            col0.Remove("1");
            col0["2"] = 22;
            col0["4"] = 44;

            // Apply non-serialized changes.
            col.ApplyChanges(col0);

            Assert.AreEqual(3, col.Count);
            Assert.AreEqual(null, col["1"]);
            Assert.AreEqual(22, col["2"]);
            Assert.AreEqual(3, col["3"]);
            Assert.AreEqual(44, col["4"]);

            // Apply serialized changes without WriteChangesOnly.
            col = getCol();
            col.ApplyChanges(SerializeDeserialize(col0));

            Assert.AreEqual(3, col.Count);
            Assert.AreEqual(null, col["1"]);
            Assert.AreEqual(22, col["2"]);
            Assert.AreEqual(3, col["3"]);
            Assert.AreEqual(44, col["4"]);

            // Apply serialized changes with WriteChangesOnly.
            col = getCol();
            col.ApplyChanges(SerializeDeserialize(col0, true));

            Assert.AreEqual(3, col.Count);
            Assert.AreEqual(null, col["1"]);
            Assert.AreEqual(22, col["2"]);
            Assert.AreEqual(3, col["3"]);
            Assert.AreEqual(44, col["4"]);

            // Remove key then add back.
            col0.Remove("2");
            col0.Remove("3");
            col0["2"] = 222;

            col = getCol();
            col.ApplyChanges(SerializeDeserialize(col0));

            Assert.AreEqual(2, col.Count);
            Assert.AreEqual(222, col["2"]);
            Assert.AreEqual(44, col["4"]);

            // Remove all.
            col0 = SerializeDeserialize(getCol());
            col0.Clear();

            col = getCol();
            col.ApplyChanges(SerializeDeserialize(col0, true));

            Assert.AreEqual(0, col.Count);

            // Add to empty.
            col0["-1"] = -1;
            col0["-2"] = -2;

            col = getCol();
            col.ApplyChanges(SerializeDeserialize(col0));

            Assert.AreEqual(2, col.Count);
            Assert.AreEqual(-1, col0["-1"]);
            Assert.AreEqual(-2, col0["-2"]);

            // Remove initial key, then add it back, then remove again.
            col0 = SerializeDeserialize(getCol());

            col0.Remove("1");
            col0.Remove("2");
            col0["1"] = "111";
            col0.Remove("1");

            col = getCol();
            col.ApplyChanges(SerializeDeserialize(col0, true));

            Assert.AreEqual(1, col.Count);
            Assert.AreEqual(3, col["3"]);
        }

        /// <summary>
        /// Serializes and deserializes back an instance.
        /// </summary>
        private static IgniteSessionStateItemCollection SerializeDeserialize(IgniteSessionStateItemCollection data, 
            bool changesOnly = false)
        {
            var marsh = BinaryUtils.Marshaller;

            using (var stream = new BinaryHeapStream(128))
            {
                var writer = marsh.StartMarshal(stream);

                data.WriteBinary(writer.GetRawWriter(), changesOnly);

                stream.Seek(0, SeekOrigin.Begin);

                return new IgniteSessionStateItemCollection(marsh.StartUnmarshal(stream));
            }
        }
    }
}
