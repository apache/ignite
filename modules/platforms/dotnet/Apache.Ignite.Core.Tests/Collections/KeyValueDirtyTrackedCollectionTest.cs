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

namespace Apache.Ignite.Core.Tests.Collections
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Collections;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="KeyValueDirtyTrackedCollection"/>
    /// </summary>
    public class KeyValueDirtyTrackedCollectionTest
    {
        /// <summary>
        /// Tests the empty collection.
        /// </summary>
        [Test]
        public void TestEmpty()
        {
            var col1 = new KeyValueDirtyTrackedCollection();
            var col2 = TestUtils.SerializeDeserialize(col1);

            foreach (var col in new[] {col1, col2})
            {
                Assert.AreEqual(0, col.Count);
                Assert.IsFalse(col.IsDirty);
                Assert.IsEmpty(col.GetKeys());

                Assert.IsNull(col["key"]);
                Assert.Throws<ArgumentOutOfRangeException>(() => col[0] = "x");
                Assert.Throws<ArgumentOutOfRangeException>(() => Assert.AreEqual(0, col[0]));
                Assert.Throws<ArgumentOutOfRangeException>(() => col.RemoveAt(0));

                col.Clear();
                col.Remove("test");

                Assert.AreEqual(0, col.Count);

                col.IsDirty = true;
                Assert.IsTrue(col.IsDirty);
            }
        }

        /// <summary>
        /// Tests the modification.
        /// </summary>
        [Test]
        public void TestModification()
        {
            var col = new KeyValueDirtyTrackedCollection();

            // Populate and check.
            col["key"] = "val";
            col["1"] = 1;

            Assert.AreEqual("val", col["key"]);
            Assert.AreEqual(1, col["1"]);

            Assert.AreEqual(2, col.Count);
            Assert.IsTrue(col.IsDirty);
            Assert.AreEqual(new[] {"key", "1"}, col.GetKeys().ToArray());

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

            // Serialize.
            var col0 = TestUtils.SerializeDeserialize(col);

            Assert.AreEqual(col.GetKeys(), col0.GetKeys());
            Assert.AreEqual(col.GetKeys().Select(x => col[x]), col0.GetKeys().Select(x => col0[x]));

            // Remove.
            col["2"] = 2;
            col["3"] = 3;

            col.Remove("invalid");
            Assert.AreEqual(4, col.Count);

            col.Remove("1");

            Assert.AreEqual(new[] { "key", "2", "3" }, col.GetKeys());
            Assert.AreEqual(null, col["1"]);

            Assert.AreEqual("val2", col["key"]);
            Assert.AreEqual("val2", col[0]);

            Assert.AreEqual(2, col["2"]);
            Assert.AreEqual(2, col[1]);

            Assert.AreEqual(3, col["3"]);
            Assert.AreEqual(3, col[2]);

            // RemoveAt.
            col0.RemoveAt(0);
            Assert.AreEqual(new[] { "1" }, col0.GetKeys());

            // Clear.
            Assert.AreEqual(3, col.Count);

            col.Clear();
            Assert.AreEqual(0, col.Count);
        }

        /// <summary>
        /// Tests dirty tracking.
        /// </summary>
        [Test]
        public void TestApplyChanges()
        {
            Func<KeyValueDirtyTrackedCollection> getCol = () =>
            {
                var res = new KeyValueDirtyTrackedCollection();

                res["1"] = 1;
                res["2"] = 2;
                res["3"] = 3;

                return res;
            };

            var col = getCol();

            var col0 = TestUtils.SerializeDeserialize(col);

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
            col.ApplyChanges(TestUtils.SerializeDeserialize(col0));

            Assert.AreEqual(3, col.Count);
            Assert.AreEqual(null, col["1"]);
            Assert.AreEqual(22, col["2"]);
            Assert.AreEqual(3, col["3"]);
            Assert.AreEqual(44, col["4"]);

            // Apply serialized changes with WriteChangesOnly.
            col0.WriteChangesOnly = true;

            col = getCol();
            col.ApplyChanges(TestUtils.SerializeDeserialize(col0));

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
            col.ApplyChanges(TestUtils.SerializeDeserialize(col0));

            Assert.AreEqual(2, col.Count);
            Assert.AreEqual(222, col["2"]);
            Assert.AreEqual(44, col["4"]);

            // Remove all.
            col0 = TestUtils.SerializeDeserialize(getCol());
            col0.WriteChangesOnly = true;
            col0.Clear();

            col = getCol();
            col.ApplyChanges(TestUtils.SerializeDeserialize(col0));

            Assert.AreEqual(0, col.Count);

            // Add to empty.
            col0["-1"] = -1;
            col0["-2"] = -2;

            col = getCol();
            col.ApplyChanges(TestUtils.SerializeDeserialize(col0));

            Assert.AreEqual(2, col.Count);
            Assert.AreEqual(-1, col0["-1"]);
            Assert.AreEqual(-2, col0["-2"]);

            // Remove initial key, then add it back, then remove again.
            col0 = TestUtils.SerializeDeserialize(getCol());
            col0.WriteChangesOnly = true;

            col0.Remove("1");
            col0.Remove("2");
            col0["1"] = "111";
            col0.Remove("1");

            col = getCol();
            col.ApplyChanges(TestUtils.SerializeDeserialize(col0));

            Assert.AreEqual(1, col.Count);
            Assert.AreEqual(3, col["3"]);
        }
    }
}
