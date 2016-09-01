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
    using Apache.Ignite.Core.Impl.Collections;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="KeyValueDirtyTrackedCollection"/>
    /// </summary>
    public class KeyValueDirtyTrackedCollectionTest
    {
        [Test]
        public void TestEmpty()
        {
            var col1 = new KeyValueDirtyTrackedCollection();
            var col2 = TestUtils.SerializeDeserialize(col1);

            foreach (var col in new[] {col1, col2})
            {
                Assert.AreEqual(0, col.Count);
                Assert.IsFalse(col.IsDirty);
                Assert.IsEmpty(col);

                Assert.IsNull(col["key"]);
                Assert.Throws<ArgumentOutOfRangeException>(() => Assert.AreEqual(0, col[1]));
                Assert.Throws<ArgumentOutOfRangeException>(() => col.RemoveAt(0));

                col.Clear();
                col.Remove("test");

                Assert.AreEqual(0, col.Count);
            }

        }

        [Test]
        public void TestSerialization()
        {
        }
    }
}
