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

            // Simple type.
            Assert.AreEqual("System.Int32", mapper.GetTypeName(typeof(int).AssemblyQualifiedName));
            Assert.AreEqual("System.Int32", mapper.GetTypeName(typeof(int).FullName));

            // Array.
            Assert.AreEqual("System.String[]", mapper.GetTypeName(typeof(string[]).AssemblyQualifiedName));
            Assert.AreEqual("System.String[]", mapper.GetTypeName(typeof(string[]).FullName));

            // Generics.
            Assert.AreEqual("System.Collections.Generic.List`1[[System.String]]", 
                mapper.GetTypeName(typeof(List<string>).AssemblyQualifiedName));
            
            Assert.AreEqual("System.Collections.Generic.Dictionary`2[[System.Int32],[System.String]]", 
                mapper.GetTypeName(typeof(Dictionary<int, string>).AssemblyQualifiedName));
            
            Assert.AreEqual("Apache.Ignite.Core.Tests.Binary.BinaryNameMapperTest+Bar`1[[Apache.Ignite.Core." +
                            "Tests.Binary.BinaryNameMapperTest+Foo]]", 
                            mapper.GetTypeName(typeof(Bar<Foo>).AssemblyQualifiedName));

            Assert.AreEqual("Apache.Ignite.Core.Tests.Binary.BinaryNameMapperTest+Bar`1[[Apache.Ignite.Core.Tests" +
                            ".Binary.BinaryNameMapperTest+Foo]][]", 
                            mapper.GetTypeName(typeof(Bar<Foo>[]).AssemblyQualifiedName));
            
            Assert.AreEqual("Apache.Ignite.Core.Tests.Binary.BinaryNameMapperTest+Bar`1[[Apache.Ignite.Core.Tests." +
                            "Binary.BinaryNameMapperTest+Foo[]]][]", 
                            mapper.GetTypeName(typeof(Bar<Foo[]>[]).AssemblyQualifiedName));

            // Open generics.
            Assert.AreEqual("System.Collections.Generic.List`1",
                mapper.GetTypeName(typeof(List<>).AssemblyQualifiedName));

            Assert.AreEqual("System.Collections.Generic.Dictionary`2",
                mapper.GetTypeName(typeof(Dictionary<,>).AssemblyQualifiedName));
        }

        /// <summary>
        /// Tests simple name mode.
        /// </summary>
        [Test]
        public void TestSimpleName()
        {
            var mapper = new BinaryBasicNameMapper {IsSimpleName = true};
                        
            // Simple type.
            Assert.AreEqual("Int32", mapper.GetTypeName(typeof(int).AssemblyQualifiedName));
            Assert.AreEqual("Int32", mapper.GetTypeName(typeof(int).FullName));

            // Array.
            Assert.AreEqual("String[]", mapper.GetTypeName(typeof(string[]).AssemblyQualifiedName));
            Assert.AreEqual("String[]", mapper.GetTypeName(typeof(string[]).FullName));

            // Generics.
            Assert.AreEqual("List`1[[String]]", mapper.GetTypeName(typeof(List<string>).AssemblyQualifiedName));
            Assert.AreEqual("Dictionary`2[[Int32],[String]]", 
                mapper.GetTypeName(typeof(Dictionary<int, string>).AssemblyQualifiedName));
            Assert.AreEqual("Bar`1[[Foo]]", mapper.GetTypeName(typeof(Bar<Foo>).AssemblyQualifiedName));
            Assert.AreEqual("Bar`1[[Foo]][]", mapper.GetTypeName(typeof(Bar<Foo>[]).AssemblyQualifiedName));
            Assert.AreEqual("Bar`1[[Foo[]]][]", mapper.GetTypeName(typeof(Bar<Foo[]>[]).AssemblyQualifiedName));
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
        // ReSharper disable once UnusedTypeParameter
        private class Bar<T>
        {
            // No-op.
        }
    }
}
