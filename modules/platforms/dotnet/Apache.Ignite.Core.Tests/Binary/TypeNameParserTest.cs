﻿/*
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

// ReSharper disable UnusedTypeParameter
namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests the type name parser.
    /// </summary>
    public class TypeNameParserTest
    {
        /// <summary>
        /// Tests simple types.
        /// </summary>
        [Test]
        public void TestSimpleTypes()
        {
            // One letter.
            var res = TypeNameParser.Parse("x");
            Assert.AreEqual("x", res.GetNameWithNamespace());
            Assert.AreEqual("x", res.GetName());
            Assert.AreEqual(0, res.NameStart);
            Assert.AreEqual(0, res.NameEnd);
            Assert.AreEqual(0, res.FullNameEnd);
            Assert.AreEqual(-1, res.AssemblyStart);
            Assert.AreEqual(-1, res.AssemblyEnd);
            Assert.IsNull(res.Generics);

            // Without assembly.
            res = TypeNameParser.Parse("System.Int");

            Assert.AreEqual(7, res.NameStart);
            Assert.AreEqual(9, res.NameEnd);
            Assert.AreEqual(9, res.FullNameEnd);
            Assert.IsNull(res.Generics);
            Assert.AreEqual(-1, res.AssemblyStart);

            // With assembly.
            res = TypeNameParser.Parse("System.Int, myasm, Ver=1");

            Assert.AreEqual(7, res.NameStart);
            Assert.AreEqual(9, res.NameEnd);
            Assert.AreEqual(9, res.FullNameEnd);
            Assert.IsNull(res.Generics);
            Assert.AreEqual(12, res.AssemblyStart);

            // Real types.
            CheckType(GetType());
            CheckType(typeof(string));
            CheckType(typeof(IDictionary));

            // Nested types.
            CheckType(typeof(Nested));
            CheckType(typeof(Nested.Nested2));
        }

        /// <summary>
        /// Tests generic types.
        /// </summary>
        [Test]
        public void TestGenericTypes()
        {
            // Simple name.
            var res = TypeNameParser.Parse("List`1[[Int]]");
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("List`1", res.GetNameWithNamespace());
            Assert.AreEqual("Int", res.Generics.Single().GetName());
            Assert.AreEqual("Int", res.Generics.Single().GetNameWithNamespace());

            // Simple name array.
            res = TypeNameParser.Parse("List`1[[Byte[]]]");
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("List`1", res.GetNameWithNamespace());
            Assert.AreEqual("Byte", res.Generics.Single().GetName());
            Assert.AreEqual("Byte", res.Generics.Single().GetNameWithNamespace());
            Assert.AreEqual("[]", res.Generics.Single().GetArray());

            // Simple name two-dimension array.
            res = TypeNameParser.Parse("List`1[[Byte[,]]]");
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("List`1", res.GetNameWithNamespace());
            Assert.AreEqual("Byte", res.Generics.Single().GetName());
            Assert.AreEqual("Byte", res.Generics.Single().GetNameWithNamespace());
            Assert.AreEqual("[,]", res.Generics.Single().GetArray());

            // Simple name jagged array.
            res = TypeNameParser.Parse("List`1[[Byte[][]]]");
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("List`1", res.GetNameWithNamespace());
            Assert.AreEqual("Byte", res.Generics.Single().GetName());
            Assert.AreEqual("Byte", res.Generics.Single().GetNameWithNamespace());
            Assert.AreEqual("[][]", res.Generics.Single().GetArray());

            // Open generic.
            res = TypeNameParser.Parse("List`1");
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("List`1", res.GetNameWithNamespace());
            Assert.IsEmpty(res.Generics);

            // One arg.
            res = TypeNameParser.Parse(typeof(List<int>).AssemblyQualifiedName);
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("System.Collections.Generic.List`1", res.GetNameWithNamespace());
            Assert.IsTrue(res.GetAssemblyName().StartsWith("mscorlib,"));

            Assert.AreEqual(1, res.Generics.Count);
            var gen = res.Generics.Single();
            Assert.AreEqual("Int32", gen.GetName());
            Assert.AreEqual("System.Int32", gen.GetNameWithNamespace());
            Assert.IsTrue(gen.GetAssemblyName().StartsWith("mscorlib,"));

            // One arg open.
            res = TypeNameParser.Parse(typeof(List<>).AssemblyQualifiedName);
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("System.Collections.Generic.List`1", res.GetNameWithNamespace());
            Assert.IsTrue(res.GetAssemblyName().StartsWith("mscorlib,"));
            Assert.IsEmpty(res.Generics);

            // Two args.
            res = TypeNameParser.Parse(typeof(Dictionary<int, string>).AssemblyQualifiedName);
            Assert.AreEqual("Dictionary`2", res.GetName());
            Assert.AreEqual("System.Collections.Generic.Dictionary`2", res.GetNameWithNamespace());
            Assert.IsTrue(res.GetAssemblyName().StartsWith("mscorlib,"));

            Assert.AreEqual(2, res.Generics.Count);

            gen = res.Generics.First();
            Assert.AreEqual("Int32", gen.GetName());
            Assert.AreEqual("System.Int32", gen.GetNameWithNamespace());
            Assert.IsTrue(gen.GetAssemblyName().StartsWith("mscorlib,"));

            gen = res.Generics.Last();
            Assert.AreEqual("String", gen.GetName());
            Assert.AreEqual("System.String", gen.GetNameWithNamespace());
            Assert.IsTrue(gen.GetAssemblyName().StartsWith("mscorlib,"));

            // Nested args.
            res = TypeNameParser.Parse(typeof(Dictionary<int, List<string>>).FullName);

            Assert.AreEqual("Dictionary`2", res.GetName());
            Assert.AreEqual("System.Collections.Generic.Dictionary`2", res.GetNameWithNamespace());
            Assert.IsNull(res.GetAssemblyName());

            Assert.AreEqual(2, res.Generics.Count);

            gen = res.Generics.Last();
            Assert.AreEqual("List`1", gen.GetName());
            Assert.AreEqual("System.Collections.Generic.List`1", gen.GetNameWithNamespace());
            Assert.IsTrue(gen.GetAssemblyName().StartsWith("mscorlib,"));
            Assert.AreEqual(1, gen.Generics.Count);

            gen = gen.Generics.Single();
            Assert.AreEqual("String", gen.GetName());
            Assert.AreEqual("System.String", gen.GetNameWithNamespace());
            Assert.IsTrue(gen.GetAssemblyName().StartsWith("mscorlib,"));

            // Nested class.
            res = TypeNameParser.Parse(typeof(NestedGeneric<int>).FullName);

            Assert.AreEqual("NestedGeneric`1", res.GetName());
            Assert.AreEqual("Apache.Ignite.Core.Tests.Binary.TypeNameParserTest+NestedGeneric`1", res.GetNameWithNamespace());

            gen = res.Generics.Single();
            Assert.AreEqual("Int32", gen.GetName());
            Assert.AreEqual("System.Int32", gen.GetNameWithNamespace());

            res = TypeNameParser.Parse(typeof(NestedGeneric<int>.NestedGeneric2<string>).AssemblyQualifiedName);
            
            Assert.AreEqual("NestedGeneric2`1", res.GetName());
            Assert.AreEqual("Apache.Ignite.Core.Tests.Binary.TypeNameParserTest+NestedGeneric`1+NestedGeneric2`1", 
                res.GetNameWithNamespace());

            Assert.AreEqual(2, res.Generics.Count);
            Assert.AreEqual("Int32", res.Generics.First().GetName());
            Assert.AreEqual("String", res.Generics.Last().GetName());
        }

        /// <summary>
        /// Tests arrays.
        /// </summary>
        [Test]
        public void TestArrays()
        {
            var res = TypeNameParser.Parse("Int32[]");
            Assert.AreEqual("Int32", res.GetName());
            Assert.AreEqual("Int32", res.GetNameWithNamespace());
            Assert.AreEqual("Int32[]", res.GetFullName());
            Assert.AreEqual("[]", res.GetArray());

            res = TypeNameParser.Parse("Int32[*]");
            Assert.AreEqual("Int32", res.GetName());
            Assert.AreEqual("Int32", res.GetNameWithNamespace());
            Assert.AreEqual("Int32[*]", res.GetFullName());
            Assert.AreEqual("[*]", res.GetArray());

            res = TypeNameParser.Parse("List`1[[Int32]][]");
            Assert.AreEqual("List`1", res.GetName());
            Assert.AreEqual("List`1", res.GetNameWithNamespace());
            Assert.AreEqual("List`1[[Int32]][]", res.GetFullName());
            Assert.AreEqual("[]", res.GetArray());

            CheckType(typeof(int[]));
            CheckType(typeof(int).MakeArrayType(1));
            CheckType(typeof(int[,]));
            CheckType(typeof(int[,,]));
            CheckType(typeof(int[][]));
            CheckType(typeof(int[,,,][,,]));

            CheckType(typeof(List<int>[]));
            CheckType(typeof(List<int>[,]));
            CheckType(typeof(List<int>[][]));
        }

        /// <summary>
        /// Tests invalid type names.
        /// </summary>
        [Test]
        public void TestInvalidTypes()
        {
            Assert.Throws<ArgumentException>(() => TypeNameParser.Parse(null));
            Assert.Throws<ArgumentException>(() => TypeNameParser.Parse(""));

            Assert.Throws<IgniteException>(() => TypeNameParser.Parse("x["));
            Assert.Throws<IgniteException>(() => TypeNameParser.Parse("x[[]"));
            Assert.Throws<IgniteException>(() => TypeNameParser.Parse("x`["));
            Assert.Throws<IgniteException>(() => TypeNameParser.Parse("x`[ ]"));
            Assert.Throws<IgniteException>(() => TypeNameParser.Parse("x,"));
            Assert.Throws<IgniteException>(() => TypeNameParser.Parse("x`2[x"));
        }

        /// <summary>
        /// Checks the type.
        /// </summary>
        private static void CheckType(Type type)
        {
            var name = type.AssemblyQualifiedName;

            Assert.IsNotNull(name);

            var res = TypeNameParser.Parse(name);

            Assert.AreEqual(type.Name, res.GetName() + res.GetArray());

            if (res.Generics == null)
            {
                Assert.AreEqual(type.FullName, res.GetNameWithNamespace() + res.GetArray());
            }

            Assert.AreEqual(type.FullName.Length + 2, res.AssemblyStart);
            Assert.AreEqual(type.FullName, res.GetFullName());
        }

        private class Nested
        {
            public class Nested2
            {
                // No-op.
            }
        }

        private class NestedGeneric<T>
        {
            public class NestedGeneric2<T2>
            {
                // No-op.
            }
        }
    }
}
