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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System;
    using System.Runtime.Serialization;
    using NUnit.Framework;

    /// <summary>
    /// Tests basic ISerializable scenarios.
    /// </summary>
    public class BasicSerializableObjectsTest
    {
        /// <summary>
        /// Tests the object with no fields.
        /// </summary>
        [Test]
        public void TestEmptyObject()
        {
            var res = TestUtils.SerializeDeserialize(new EmptyObject());

            Assert.IsNotNull(res);
        }

        /// <summary>
        /// Tests the object with no fields.
        /// </summary>
        [Test]
        public void TestEmptyObjectOnline()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            using (var ignite2 = Ignition.Start(TestUtils.GetTestConfiguration(name: "1")))
            {
                var cache = ignite.CreateCache<int, EmptyObject>("c");

                cache[1] = new EmptyObject();

                var res = ignite2.GetCache<int, EmptyObject>("c")[1];

                Assert.IsNotNull(res);
            }
        }

        /// <summary>
        /// Tests ISerializable without serialization ctor.
        /// </summary>
        [Test]
        public void TestMissingCtor()
        {
            var ex = Assert.Throws<SerializationException>(() => TestUtils.SerializeDeserialize(new MissingCtor()));
            Assert.AreEqual(string.Format("The constructor to deserialize an object of type '{0}' was not found.", 
                typeof(MissingCtor)), ex.Message);
        }

        /// <summary>
        /// Tests <see cref="Type"/> serialization.
        /// </summary>
        [Test]
        public void TestTypes()
        {
            var type = GetType();

            var res = TestUtils.SerializeDeserialize(type);

            Assert.AreEqual(type, res);
        }

        /// <summary>
        /// Missing serialization ctor.
        /// </summary>
        private class MissingCtor : ISerializable
        {
            /** <inheritdoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Object with no fields.
        /// </summary>
        [Serializable]
        private class EmptyObject : ISerializable
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="EmptyObject"/> class.
            /// </summary>
            public EmptyObject()
            {
                // No-op.
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="EmptyObject"/> class.
            /// </summary>
            private EmptyObject(SerializationInfo info, StreamingContext context)
            {
                Assert.AreEqual(StreamingContextStates.All, context.State);
                Assert.IsNull(context.Context);
            }

            /** <inheritdoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                Assert.AreEqual(StreamingContextStates.All, context.State);
                Assert.IsNull(context.Context);
            }
        }
    }
}
