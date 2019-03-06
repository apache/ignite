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
    /// Tests that <see cref="IObjectReference"/> objects are deserialized properly.
    /// This only applies to <see cref="ISerializable"/> implementers, which can replace underlying object
    /// with <see cref="SerializationInfo.SetType"/>, <see cref="SerializationInfo.AssemblyName"/>, and
    /// <see cref="SerializationInfo.FullTypeName"/>.
    /// </summary>
    public class ObjectReferenceTests
    {
        /// <summary>
        /// Tests serialization object replacement with <see cref="SerializationInfo.SetType"/> method.
        /// </summary>
        [Test]
        public void TestSetType()
        {
            var obj = new SetTypeReplacer(25);

            var res = TestUtils.SerializeDeserialize(obj);

            Assert.AreEqual(obj.Value, res.Value);
        }

        /// <summary>
        /// Tests serialization object replacement with <see cref="SerializationInfo.FullTypeName"/> property.
        /// </summary>
        [Test]
        public void TestTypeName()
        {
            var obj = new TypeNameReplacer(36);

            var res = TestUtils.SerializeDeserialize(obj);

            Assert.AreEqual(obj.Value, res.Value);
        }

        [Serializable]
        private class SetTypeReplacer : ISerializable
        {
            private readonly int _value;

            public SetTypeReplacer(int value)
            {
                _value = value;
            }

            public int Value
            {
                get { return _value; }
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.SetType(typeof(ObjectInfoHolder));

                info.AddValue("type", GetType());
                info.AddValue("val", Value);
            }
        }

        [Serializable]
        private class TypeNameReplacer : ISerializable
        {
            private readonly int _value;

            public TypeNameReplacer(int value)
            {
                _value = value;
            }

            public int Value
            {
                get { return _value; }
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.FullTypeName = typeof(ObjectInfoHolder).FullName;
                info.AssemblyName = typeof(ObjectInfoHolder).Assembly.FullName;

                info.AddValue("type", GetType());
                info.AddValue("val", Value);
            }
        }

        [Serializable]
        private class ObjectInfoHolder : IObjectReference, ISerializable
        {
            public Type ObjectType { get; set; }

            public int Value { get; set; }

            public object GetRealObject(StreamingContext context)
            {
                return Activator.CreateInstance(ObjectType, Value);
            }

            public ObjectInfoHolder(SerializationInfo info, StreamingContext context)
            {
                ObjectType = (Type) info.GetValue("type", typeof(Type));
                Value = info.GetInt32("val");
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                // No-op.
            }
        }
    }
}
