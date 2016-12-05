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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Equality comparers test.
    /// </summary>
    public class BinaryEqualityComparerTest
    {
        /// <summary>
        /// Public methods should throw unsupported exceptions.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed")]
        public void TestPublicMethods()
        {
            var cmps = new IEqualityComparer<IBinaryObject>[]
            {
                new BinaryArrayEqualityComparer(),
                new BinaryFieldEqualityComparer()
            };

            foreach (var cmp in cmps)
            {
                Assert.Throws<NotSupportedException>(() => cmp.Equals(null, null));
                Assert.Throws<NotSupportedException>(() => cmp.GetHashCode(null));
            }
        }

        /// <summary>
        /// Tests the array comparer.
        /// </summary>
        [Test]
        public void TestArrayComparer()
        {
            var cmp = (IBinaryEqualityComparer) new BinaryArrayEqualityComparer();

            var ms = new BinaryHeapStream(10);

            Assert.AreEqual(1, cmp.GetHashCode(ms, 0, 0, null, 0, null, null));

            ms.WriteByte(1);
            Assert.AreEqual(31 + 1, cmp.GetHashCode(ms, 0, 1, null, 0, null, null));

            ms.WriteByte(3);
            Assert.AreEqual((31 + 1) * 31 + 3, cmp.GetHashCode(ms, 0, 2, null, 0, null, null));
        }

        /// <summary>
        /// Tests the field comparer.
        /// </summary>
        [Test]
        public void TestFieldComparer()
        {
            var cmp = (IBinaryEqualityComparer)new BinaryFieldEqualityComparer();

            // TODO: Marshal and unmarshal as binary, check resulting hash code. Should we do the same for Array?
        }
    }
}
