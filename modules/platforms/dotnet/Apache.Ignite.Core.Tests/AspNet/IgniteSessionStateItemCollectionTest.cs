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
    using Apache.Ignite.AspNet.Impl;
    using Apache.Ignite.Core.Impl.Collections;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteSessionStateItemCollection"/>.
    /// </summary>
    public class IgniteSessionStateItemCollectionTest
    {
        /// <summary>
        /// Tests the collection.
        /// </summary>
        [Test]
        public void TestCollection()
        {
            var innerCol = new KeyValueDirtyTrackedCollection();
            var col = new IgniteSessionStateItemCollection(innerCol);

            // Check empty.
            Assert.IsFalse(col.Dirty);
            Assert.IsFalse(col.IsSynchronized);
            Assert.AreEqual(0, col.Count);
            Assert.Throws<NotSupportedException>(() => Assert.IsNull(col.Keys));
            Assert.IsNotNull(col.SyncRoot);
            Assert.IsEmpty(col);
            col.Clear();

            var keys = new[] {"1"};
            col.CopyTo(keys, 0);
            Assert.AreEqual(new[] {"1"}, keys);
        }
    }
}
