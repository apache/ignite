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
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests that deserialization callbacks are invoked correctly.
    /// </summary>
    public class CallbacksTest
    {
        /** Deserialization messages. */
        private static readonly List<string> Messages = new List<string>();

        /// <summary>
        /// Tests that callbacks are invoked in correct order on class with ISerializable interface.
        /// </summary>
        [Test]
        public void TestSerializable()
        {
            CheckCallbacks<SerCallbacks>(true);
        }

        /// <summary>
        /// Tests that callbacks are invoked in correct order on class without ISerializable interface.
        /// </summary>
        [Test]
        public void TestNonSerializable()
        {
            CheckCallbacks<SerCallbacksNoInterface>(false);
        }

        /// <summary>
        /// Tests that callbacks are invoked in correct order on class with ISerializable interface.
        /// </summary>
        [Test]
        public void TestSerializableStruct()
        {
            var obj = new SerCallbacksStruct
            {
                Name = "Foo",
                Inner = new SerCallbacksStruct
                {
                    Name = "Bar"
                }
            };

            Messages.Clear();
            var res = TestUtils.SerializeDeserialize(obj);

            Assert.AreEqual("Foo", res.Name);
            Assert.AreEqual("Bar", ((SerCallbacksStruct) res.Inner).Name);

            // OnDeserialization callbacks should be called AFTER entire tree is deserialized.
            // Other callbacks order is not strictly defined.
            var expected = new[]
            {
                "Foo.OnSerializing",
                "Bar.OnSerializing",
                "Bar.OnSerialized",
                "Foo.OnSerialized",
                ".OnDeserializing",
                ".OnDeserializing",
                "Bar.ctor",
                "Bar.OnDeserialized",
                "Foo.ctor",
                "Foo.OnDeserialized",
                "Foo.OnDeserialization",
                "Bar.OnDeserialization",
            };

            Assert.AreEqual(expected, Messages);
        }

        /// <summary>
        /// Tests that callbacks are invoked in correct order on class without ISerializable interface.
        /// </summary>
        [Test]
        public void TestNonSerializableStruct()
        {
            var obj = new SerCallbacksStructNoInterface
            {
                Name = "Foo",
                Inner = new SerCallbacksStructNoInterface
                {
                    Name = "Bar"
                }
            };

            Messages.Clear();
            var res = TestUtils.SerializeDeserialize(obj);

            Assert.AreEqual("Foo", res.Name);
            Assert.AreEqual("Bar", ((SerCallbacksStructNoInterface) res.Inner).Name);

            // OnDeserialization callbacks should be called AFTER entire tree is deserialized.
            // Other callbacks order is not strictly defined.
            var expected = new[]
            {
                "Foo.OnSerializing",
                "Bar.OnSerializing",
                "Bar.OnSerialized",
                "Foo.OnSerialized",
                ".OnDeserializing",
                ".OnDeserializing",
                "Bar.OnDeserialized",
                "Foo.OnDeserialized",
                "Foo.OnDeserialization",
                "Bar.OnDeserialization",
            };

            Assert.AreEqual(expected, Messages);
        }

        /// <summary>
        /// Checks the callbacks.
        /// </summary>
        private static void CheckCallbacks<T>(bool ctorCall) where T : SerCallbacksNoInterface, new()
        {
            var obj = new T
            {
                Name = "Foo",
                Inner = new T
                {
                    Name = "Bar",
                    Inner = new T
                    {
                        Name = "Baz"
                    }
                }
            };

            Messages.Clear();
            var res = TestUtils.SerializeDeserialize(obj);

            Assert.AreEqual("Foo", res.Name);
            Assert.AreEqual("Bar", res.Inner.Name);
            Assert.AreEqual("Baz", res.Inner.Inner.Name);

            // OnDeserialization callbacks should be called AFTER entire tree is deserialized.
            // Other callbacks order is not strictly defined.
            var expected = new[]
            {
                "Foo.OnSerializing",
                "Bar.OnSerializing",
                "Baz.OnSerializing",
                "Baz.OnSerialized",
                "Bar.OnSerialized",
                "Foo.OnSerialized",
                ".OnDeserializing",
                ".OnDeserializing",
                ".OnDeserializing",
                "Baz.ctor",
                "Baz.OnDeserialized",
                "Bar.ctor",
                "Bar.OnDeserialized",
                "Foo.ctor",
                "Foo.OnDeserialized",
                "Foo.OnDeserialization",
                "Bar.OnDeserialization",
                "Baz.OnDeserialization"
            };

            if (!ctorCall)
                expected = expected.Where(x => !x.Contains("ctor")).ToArray();

            Assert.AreEqual(expected, Messages);
        }

        /// <summary>
        /// Tests that incorrect method signature causes a descriptive exception.
        /// </summary>
        [Test]
        public void TestIncorrectMethodSignature()
        {
            var ex = Assert.Catch(() => TestUtils.SerializeDeserialize(new InvalidCallbackSignature()));
            
            var tex = ex as TargetInvocationException;
            ex = tex != null ? tex.InnerException : ex;

            Assert.IsNotNull(ex);

            var t = typeof(InvalidCallbackSignature);

            Assert.AreEqual(string.Format("Type '{0}' in assembly '{1}' has method 'OnDeserializing' " +
                                          "with an incorrect signature for the serialization attribute that it " +
                                          "is decorated with.", t, t.Assembly), ex.Message);
        }

        /// <summary>
        /// Class with serialization callbacks and <see cref="ISerializable" /> implemented.
        /// This goes through <see cref="SerializableSerializer"/>.
        /// </summary>
        [Serializable]
        private class SerCallbacks : SerCallbacksNoInterface, ISerializable
        {
            public SerCallbacks()
            {
            }

            protected SerCallbacks(SerializationInfo info, StreamingContext context)
            {
                Name = info.GetString("name");
                Inner = (SerCallbacks) info.GetValue("inner", typeof(SerCallbacks));

                Messages.Add(string.Format("{0}.ctor", Name));
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("name", Name);
                info.AddValue("inner", Inner);
            }
        }

        /// <summary>
        /// Class with serialization callbacks and without <see cref="ISerializable" /> implemented.
        /// This goes through <see cref="BinaryReflectiveSerializer"/>.
        /// </summary>
        [Serializable]
        private class SerCallbacksNoInterface : IDeserializationCallback
        {
            public string Name { get; set; }

            public SerCallbacksNoInterface Inner { get; set; }

            public void OnDeserialization(object sender)
            {
                Messages.Add(string.Format("{0}.OnDeserialization", Name));
            }

            [OnSerializing]
            public void OnSerializing(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnSerializing", Name));
            }

            [OnSerialized]
            public void OnSerialized(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnSerialized", Name));
            }

            [OnDeserializing]
            public void OnDeserializing(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnDeserializing", Name));
            }

            [OnDeserialized]
            public void OnDeserialized(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnDeserialized", Name));
            }
        }

        private class InvalidCallbackSignature
        {
            [OnDeserializing]
            public void OnDeserializing()
            {
                // No-op.
            }
        }

        [Serializable]
        private struct SerCallbacksStruct : IDeserializationCallback, ISerializable
        {
            public string Name { get; set; }

            public object Inner { get; set; }

            public SerCallbacksStruct(SerializationInfo info, StreamingContext context) : this()
            {
                Name = info.GetString("name");
                Inner = info.GetValue("inner", typeof(object));
                Messages.Add(string.Format("{0}.ctor", Name));
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("name", Name);
                info.AddValue("inner", Inner);
            }

            public void OnDeserialization(object sender)
            {
                Messages.Add(string.Format("{0}.OnDeserialization", Name));
            }

            [OnSerializing]
            public void OnSerializing(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnSerializing", Name));
            }

            [OnSerialized]
            public void OnSerialized(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnSerialized", Name));
            }

            [OnDeserializing]
            public void OnDeserializing(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnDeserializing", Name));
            }

            [OnDeserialized]
            public void OnDeserialized(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnDeserialized", Name));
            }
        }

        private struct SerCallbacksStructNoInterface : IDeserializationCallback
        {
            public string Name { get; set; }

            public object Inner { get; set; }

            public void OnDeserialization(object sender)
            {
                Messages.Add(string.Format("{0}.OnDeserialization", Name));
            }

            [OnSerializing]
            public void OnSerializing(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnSerializing", Name));
            }

            [OnSerialized]
            public void OnSerialized(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnSerialized", Name));
            }

            [OnDeserializing]
            public void OnDeserializing(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnDeserializing", Name));
            }

            [OnDeserialized]
            public void OnDeserialized(StreamingContext context)
            {
                Messages.Add(string.Format("{0}.OnDeserialized", Name));
            }
        }
    }
}
