/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Common
{
    using System;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests the <see cref="IgniteGuid"/>.
    /// </summary>
    public class IgniteGuidTest
    {
        /// <summary>
        /// Tests the <see cref="IgniteGuid"/>.
        /// </summary>
        [Test]
        public void TestIgniteGuid()
        {
            var guid = Guid.NewGuid();

            var id1 = new IgniteGuid(guid, 1);
            var id2 = new IgniteGuid(guid, 1);
            var id3 = new IgniteGuid(guid, 2);
            var id4 = new IgniteGuid(Guid.NewGuid(), 2);

            // Properties.
            Assert.AreEqual(guid, id1.GlobalId);
            Assert.AreEqual(1, id1.LocalId);
            Assert.AreEqual(id1.GetHashCode(), id2.GetHashCode());

            // Equality.
            Assert.AreEqual(id1, id2);
            Assert.IsTrue(id1 == id2);
            Assert.IsFalse(id1 != id2);

            // Inequality.
            Assert.AreNotEqual(id1, id3);
            Assert.IsFalse(id1 == id3);
            Assert.IsTrue(id1 != id3);

            Assert.AreNotEqual(id4, id3);
            Assert.IsFalse(id4 == id3);
            Assert.IsTrue(id4 != id3);
        }
    }
}
