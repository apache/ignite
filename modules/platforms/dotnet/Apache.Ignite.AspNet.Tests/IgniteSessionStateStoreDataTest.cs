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

namespace Apache.Ignite.AspNet.Tests
{
    using System;
    using System.IO;
    using System.Reflection;
    using System.Web;
    using Apache.Ignite.AspNet.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteSessionStateStoreData"/>.
    /// </summary>
    public class IgniteSessionStateStoreDataTest
    {
        /// <summary>
        /// Tests the data.
        /// </summary>
        [Test]
        public void TestData()
        {
            // Modification method is internal.
            var statics = new HttpStaticObjectsCollection();
            statics.GetType().GetMethod("Add", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(statics, new object[] { "int", typeof(int), false });

            var data = new IgniteSessionStateStoreData(statics, 44);

            data.Items["key"] = "val";

            Assert.AreEqual(44, data.Timeout);
            Assert.AreEqual(1, data.StaticObjects.Count);
            Assert.AreEqual(0, data.StaticObjects["int"]);
            Assert.AreEqual("val", data.Items["key"]);
        }

        /// <summary>
        /// Tests the empty data.
        /// </summary>
        [Test]
        public void TestEmpty()
        {
            var data = new IgniteSessionStateStoreData(null, 0);

            Assert.AreEqual(0, data.LockId);
            Assert.AreEqual(0, data.Items.Count);
            Assert.AreEqual(0, data.Timeout);
            Assert.IsNull(data.LockNodeId);
            Assert.IsNull(data.LockTime);
            Assert.IsNull(data.StaticObjects);
        }

        /// <summary>
        /// Tests the serialization.
        /// </summary>
        [Test]
        public void TestSerialization()
        {
            var data = new IgniteSessionStateStoreData(null, 96)
            {
                Timeout = 97,
                LockId = 11,
                LockNodeId = Guid.NewGuid(),
                LockTime = DateTime.UtcNow.AddHours(-1),
            };

            data.Items["key1"] = 1;
            data.Items["key2"] = 2;

            var data0 = SerializeDeserialize(data);

            Assert.AreEqual(data.Timeout, data0.Timeout);
            Assert.AreEqual(data.LockId, data0.LockId);
            Assert.AreEqual(data.LockNodeId, data0.LockNodeId);
            Assert.AreEqual(data.LockTime, data0.LockTime);
            Assert.AreEqual(data.Items.Keys, data0.Items.Keys);
        }


        /// <summary>
        /// Serializes and deserializes back an instance.
        /// </summary>
        private static IgniteSessionStateStoreData SerializeDeserialize(IgniteSessionStateStoreData data)
        {
            var marsh = BinaryUtils.Marshaller;

            using (var stream = new BinaryHeapStream(128))
            {
                var writer = marsh.StartMarshal(stream);

                data.WriteBinary(writer.GetRawWriter(), false);

                stream.Seek(0, SeekOrigin.Begin);

                return new IgniteSessionStateStoreData(marsh.StartUnmarshal(stream));
            }
        }
    }
}
