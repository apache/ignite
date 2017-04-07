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
    using System;
    using System.Collections;
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
            // Without assembly.
            var res = TypeNameParser.Parse("System.Int");

            Assert.AreEqual(7, res.NameStart);
            Assert.AreEqual(9, res.NameEnd);
            Assert.IsNull(res.Generics);
            Assert.AreEqual(-1, res.AssemblyIndex);

            // With assembly.
            res = TypeNameParser.Parse("System.Int, myasm, Ver=1");

            Assert.AreEqual(7, res.NameStart);
            Assert.AreEqual(9, res.NameEnd);
            Assert.IsNull(res.Generics);
            Assert.AreEqual(11, res.AssemblyIndex);

            // Real types.
            CheckType(GetType());
            CheckType(typeof(string));
            CheckType(typeof(IDictionary));
        }

        /// <summary>
        /// Tests generic types.
        /// </summary>
        [Test]
        public void TestGenericTypes()
        {
            // One arg.

            // Two args.

            // Nested args.
        }

        /// <summary>
        /// Tests arrays.
        /// </summary>
        [Test]
        public void TestArrays()
        {
            // TODO
        }

        /// <summary>
        /// Checks the type.
        /// </summary>
        private static void CheckType(Type type)
        {
            var res = TypeNameParser.Parse(type.AssemblyQualifiedName);

            Assert.IsNotNull(type.Namespace);
            Assert.AreEqual(type.Namespace.Length + 1, res.NameStart);
            Assert.AreEqual(type.Namespace.Length + type.Name.Length, res.NameEnd);
            Assert.AreEqual(type.FullName.Length + 1, res.AssemblyIndex);
        }
    }
}
