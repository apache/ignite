/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using Apache.Ignite.Core.Cache.Affinity;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="AffinityKey"/>
    /// </summary>
    public class AffinityKeyTest
    {
        /// <summary>
        /// Tests the equality.
        /// </summary>
        [Test]
        public void TestEquality()
        {
            // Default.
            var key = new AffinityKey();

            Assert.IsNull(key.Key);
            Assert.IsNull(key.Affinity);
            Assert.AreEqual(0, key.GetHashCode());
            Assert.AreEqual(new AffinityKey(), key);

            // Ctor 1.
            const string myKey = "myKey";
            key = new AffinityKey(myKey);

            Assert.AreEqual(myKey, key.Key);
            Assert.AreEqual(myKey, key.Affinity);
            Assert.AreNotEqual(0, key.GetHashCode());

            // Ctor 2.
            var ver1 = new AffinityKey(long.MaxValue, int.MaxValue);
            var ver2 = new AffinityKey(long.MaxValue, int.MaxValue);

            Assert.AreEqual(ver1, ver2);
            Assert.IsTrue(ver1 == ver2);
            Assert.IsFalse(ver1 != ver2);

            Assert.AreNotEqual(key, ver1);
            Assert.IsTrue(key != ver1);
            Assert.IsFalse(key == ver1);

            // ToString.
            Assert.AreEqual("AffinityKey [Key=1, Affinity=2]", new AffinityKey(1, 2).ToString());
        }
    }
}