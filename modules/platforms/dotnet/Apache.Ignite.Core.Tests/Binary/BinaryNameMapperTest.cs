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
    using Apache.Ignite.Platform.Model;
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
        /// Tests BinaryBasicNameMapperForJava.
        /// </summary>
        [Test]
        public void TestBinaryBasicNameMapperForJava()
        {
            var mapper = new BinaryBasicNameMapper {NamespaceToLower = true};
            Assert.IsFalse(mapper.IsSimpleName);

            Assert.AreEqual("org.company.Class", mapper.GetTypeName("Org.Company.Class"));
            Assert.AreEqual("org.mycompany.Class", mapper.GetTypeName("Org.MyCompany.Class"));
            Assert.AreEqual("org.company.MyClass", mapper.GetTypeName("Org.Company.MyClass"));
            Assert.AreEqual("org.company.URL", mapper.GetTypeName("Org.Company.URL"));

            Assert.AreEqual("apache.ignite.platform.model.Address", 
                mapper.GetTypeName(typeof(Address).FullName));

            Assert.AreEqual("apache.ignite.platform.model.Address[]", 
                mapper.GetTypeName(typeof(Address[]).FullName));

            Assert.AreEqual("system.collections.generic.List`1",
                mapper.GetTypeName(typeof(List<>).AssemblyQualifiedName));

            Assert.AreEqual("system.collections.generic.List`1[[apache.ignite.platform.model.Address]]",
                mapper.GetTypeName(typeof(List<Address>).AssemblyQualifiedName));

            Assert.AreEqual("system.collections.generic.Dictionary`2",
                mapper.GetTypeName(typeof(Dictionary<,>).AssemblyQualifiedName));

            Assert.AreEqual("system.collections.generic.Dictionary`2[[system.Int32],[apache.ignite.platform.model.Address]]",
                mapper.GetTypeName(typeof(Dictionary<int,Address>).AssemblyQualifiedName));
        }

        /// <summary>
        /// Tests BinaryBasicNameMapperForJava with simple name mode.
        /// </summary>
        [Test]
        public void TestBinaryBasicNameMapperForJavaSimpleName()
        {
            var mapper = new BinaryBasicNameMapper {IsSimpleName = true, NamespaceToLower = true};
            Assert.IsTrue(mapper.IsSimpleName);

            Assert.AreEqual("Class", mapper.GetTypeName("Org.Company.Class"));
            Assert.AreEqual("Class", mapper.GetTypeName("Org.MyCompany.Class"));
            Assert.AreEqual("MyClass", mapper.GetTypeName("Org.Company.MyClass"));
            Assert.AreEqual("URL", mapper.GetTypeName("Org.Company.URL"));

            Assert.AreEqual("List`1",
                mapper.GetTypeName(typeof(List<>).AssemblyQualifiedName));

            Assert.AreEqual("List`1[[Address]]",
                mapper.GetTypeName(typeof(List<Address>).AssemblyQualifiedName));

            Assert.AreEqual("Dictionary`2",
                mapper.GetTypeName(typeof(Dictionary<,>).AssemblyQualifiedName));

            Assert.AreEqual("Dictionary`2[[Int32],[Address]]",
                mapper.GetTypeName(typeof(Dictionary<int,Address>).AssemblyQualifiedName));

            mapper = new BinaryBasicNameMapper {IsSimpleName = true, NamespacePrefix = "org."};
            Assert.IsTrue(mapper.IsSimpleName);

            Assert.AreEqual("Class", mapper.GetTypeName("Org.Company.Class"));
            Assert.AreEqual("Class", mapper.GetTypeName("Org.MyCompany.Class"));
            Assert.AreEqual("MyClass", mapper.GetTypeName("Org.Company.MyClass"));
            Assert.AreEqual("URL", mapper.GetTypeName("Org.Company.URL"));

            Assert.AreEqual("List`1",
                mapper.GetTypeName(typeof(List<>).AssemblyQualifiedName));

            Assert.AreEqual("List`1[[Address]]",
                mapper.GetTypeName(typeof(List<Address>).AssemblyQualifiedName));

            Assert.AreEqual("Dictionary`2",
                mapper.GetTypeName(typeof(Dictionary<,>).AssemblyQualifiedName));

            Assert.AreEqual("Dictionary`2[[Int32],[Address]]",
                mapper.GetTypeName(typeof(Dictionary<int, Address>).AssemblyQualifiedName));
        }

        /// <summary>
        /// Tests BinaryBasicNameMapperForJava and JavaDomain = "org".
        /// </summary>
        [Test]
        public void TestFullNameForceJavaNamingConventionsWithDomain()
        {
            var mapper = new BinaryBasicNameMapper {NamespacePrefix = "org.", NamespaceToLower = true};
            Assert.IsFalse(mapper.IsSimpleName);

            Assert.AreEqual("org.company.Class", mapper.GetTypeName("Company.Class"));
            Assert.AreEqual("org.mycompany.Class", mapper.GetTypeName("MyCompany.Class"));
            Assert.AreEqual("org.company.MyClass", mapper.GetTypeName("Company.MyClass"));
            Assert.AreEqual("org.company.URL", mapper.GetTypeName("Company.URL"));

            Assert.AreEqual("org.apache.ignite.platform.model.Address", 
                mapper.GetTypeName(typeof(Address).FullName));

            Assert.AreEqual("org.apache.ignite.platform.model.Address[]", 
                mapper.GetTypeName(typeof(Address[]).FullName));

            Assert.AreEqual("org.system.collections.generic.List`1[[org.apache.ignite.platform.model.Address]]",
                mapper.GetTypeName(typeof(List<Address>).AssemblyQualifiedName));

            Assert.AreEqual("org.system.collections.generic.List`1",
                mapper.GetTypeName(typeof(List<>).AssemblyQualifiedName));

            Assert.AreEqual("org.system.collections.generic.Dictionary`2",
                mapper.GetTypeName(typeof(Dictionary<,>).AssemblyQualifiedName));

            Assert.AreEqual("org.system.collections.generic.Dictionary`2[[org.system.Int32],[org.apache.ignite.platform.model.Address]]",
                mapper.GetTypeName(typeof(Dictionary<int, Address>).AssemblyQualifiedName));

            mapper = new BinaryBasicNameMapper {NamespacePrefix = "Org.", NamespaceToLower = false};
            Assert.IsFalse(mapper.IsSimpleName);

            Assert.AreEqual("Org.Company.Class", mapper.GetTypeName("Company.Class"));
            Assert.AreEqual("Org.MyCompany.Class", mapper.GetTypeName("MyCompany.Class"));
            Assert.AreEqual("Org.Company.MyClass", mapper.GetTypeName("Company.MyClass"));
            Assert.AreEqual("Org.Company.URL", mapper.GetTypeName("Company.URL"));

            Assert.AreEqual("Org.Apache.Ignite.Platform.Model.Address", 
                mapper.GetTypeName(typeof(Address).FullName));

            Assert.AreEqual("Org.Apache.Ignite.Platform.Model.Address[]", 
                mapper.GetTypeName(typeof(Address[]).FullName));

            Assert.AreEqual("Org.System.Collections.Generic.List`1[[Org.Apache.Ignite.Platform.Model.Address]]",
                mapper.GetTypeName(typeof(List<Address>).AssemblyQualifiedName));

            Assert.AreEqual("Org.System.Collections.Generic.List`1",
                mapper.GetTypeName(typeof(List<>).AssemblyQualifiedName));

            Assert.AreEqual("Org.System.Collections.Generic.Dictionary`2",
                mapper.GetTypeName(typeof(Dictionary<,>).AssemblyQualifiedName));

            Assert.AreEqual("Org.System.Collections.Generic.Dictionary`2[[Org.System.Int32],[Org.Apache.Ignite.Platform.Model.Address]]",
                mapper.GetTypeName(typeof(Dictionary<int, Address>).AssemblyQualifiedName));
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
