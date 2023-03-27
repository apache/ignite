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
                // ReSharper disable once AssignNullToNotNullAttribute
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
