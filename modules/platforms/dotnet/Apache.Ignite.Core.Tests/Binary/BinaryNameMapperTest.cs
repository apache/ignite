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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests binary name mapper.
    /// </summary>
    public class BinaryNameMapperTest
    {
        /// <summary>
        /// Tests full name mode.
        /// </summary>
        [Test]
        public void TestFullName()
        {
            var mapper = new BinaryBasicNameMapper();

            Assert.IsFalse(mapper.IsSimpleName);

            foreach (var type in new[] {GetType(), typeof(Foo)})
            {
                Assert.AreEqual(type.FullName, mapper.GetTypeName(type.AssemblyQualifiedName));
                Assert.AreEqual(type.FullName, mapper.GetTypeName(type.FullName));
                Assert.AreEqual(type.Name, mapper.GetTypeName(type.Name));
            }
        }

        /// <summary>
        /// Tests simple name mode.
        /// </summary>
        [Test]
        public void TestSimpleName()
        {
            var mapper = new BinaryBasicNameMapper {IsSimpleName = true};

            Assert.IsTrue(mapper.IsSimpleName);

            foreach (var type in new[] {GetType(), typeof(Foo), typeof(int)})
            {
                Assert.AreEqual(type.Name, mapper.GetTypeName(type.AssemblyQualifiedName));
                Assert.AreEqual(type.Name, mapper.GetTypeName(type.FullName));
                Assert.AreEqual(type.Name, mapper.GetTypeName(type.Name));
            }

            // Generics.
            Assert.AreEqual("List[String]", mapper.GetTypeName(typeof(List<string>).AssemblyQualifiedName));
            Assert.AreEqual("Dictionary[Int32,String]", 
                mapper.GetTypeName(typeof(Dictionary<int, string>).AssemblyQualifiedName));
            Assert.AreEqual("Bar[Foo]", mapper.GetTypeName(typeof(Bar<Foo>).AssemblyQualifiedName));
        }

        /// <summary>
        /// Nested class.
        /// </summary>
        private class Foo
        {
            // No-op.
        }

        /// <summary>
        /// Nested generic class.
        /// </summary>
        private class Bar<T>
        {
            // No-op.
        }
    }
}
