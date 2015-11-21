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

namespace Apache.Ignite.Core.Tests.Common
{
    using System;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="SerializableWrapper{T}"/>.
    /// </summary>
    [Serializable]
    public class SerializableWrapperTest
    {
        /** */
        private int field = 1;

        /** */
        private readonly object _nullField = null;

        /// <summary>
        /// Tests static delegates (target = null) serialization.
        /// </summary>
        [Test]
        public void TestStaticDelegates()
        {
            Func<int> del = () => 17;
            Assert.AreEqual(17, SerializeDeserialize(del)());

            del = StaticMethod;
            Assert.AreEqual(42, SerializeDeserialize(del)());
        }

        /// <summary>
        /// Tests instance delegates (instance methods or anonymous delegates that capture fields).
        /// </summary>
        [Test]
        public void TestFieldCapture()
        {
            Func<int> del = () => field;
            Assert.AreEqual(field, SerializeDeserialize(del)());

            Func<object> del2 = () => _nullField;
            Assert.IsNull(SerializeDeserialize(del2)());

            del = InstanceMethod;
            Assert.AreEqual(field, SerializeDeserialize(del)());
        }

        /// <summary>
        /// Tests delegates that capture variables.
        /// </summary>
        [Test]
        public void TestVariableCapture()
        {
            var x = 15;
            var y = 89;

            Func<int> del = () => x + y;
            Assert.AreEqual(x + y, SerializeDeserialize(del)());

            Func<int> del2 = () => x - y;
            Assert.AreEqual(x - y, SerializeDeserialize(del2)());

            // Loop variable
            for (int i = 0; i < 100; i++)
            {
                // ReSharper disable once AccessToModifiedClosure
                Func<int> del3 = () => x - i;
                Assert.AreEqual(x - i, SerializeDeserialize(del3)());
            }

            // Null
            string nullString = null;

            // ReSharper disable once ExpressionIsAlwaysNull
            Func<string> del4 = () => nullString;
            Assert.IsNull(SerializeDeserialize(del4)());
        }

        /// <summary>
        /// Tests nested delegates.
        /// </summary>
        [Test]
        public void TestNestedDelegates()
        {
            var x = 56;

            Func<int> del = () => x + 1;
            Func<string> del2 = () => del().ToString();

            // Nested delegates are not supported
            Assert.Throws<SerializationException>(() => SerializeDeserialize(del2)());
        }

        /// <summary>
        /// Tests chained delegates.
        /// </summary>
        [Test]
        public void TestChainedDelegates()
        {
            var x = 56;

            Func<int> del = () => x + 1;

            del += () => x + 2;

            // Chained delegates are not supported
            Assert.Throws<SerializationException>(() => SerializeDeserialize(del)());
        }

        /// <summary>
        /// Void method.
        /// </summary>
        private static int StaticMethod()
        {
            return 42;
        }

        /// <summary>
        /// Instance method.
        /// </summary>
        private int InstanceMethod()
        {
            return field;
        }

        /// <summary>
        /// Serializes and deserializes an object.
        /// </summary>
        private static T SerializeDeserialize<T>(T obj)
        {
            var bf = new BinaryFormatter();

            var holder = new SerializableWrapper<T>(obj);

            using (var memStream = new MemoryStream())
            {
                bf.Serialize(memStream, holder);

                memStream.Seek(0, SeekOrigin.Begin);

                var holder0 = (SerializableWrapper<T>)bf.Deserialize(memStream);

                return holder0.WrappedObject;
            }
        }
    }
}
