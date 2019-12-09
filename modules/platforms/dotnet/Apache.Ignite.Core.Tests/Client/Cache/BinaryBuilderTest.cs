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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests binary object builder in thin client.
    /// </summary>
    public class BinaryBuilderTest : ClientTestBase
    {
        /// <summary>
        /// Tests the classless object builder.
        /// </summary>
        [Test]
        public void TestClasslessBuilder([Values(true, false)] bool useTypeOverrideSetters)
        {
            var bin = Client.GetBinary();

            var typeName = "FooBarBaz_" + useTypeOverrideSetters;
            
            var obj = useTypeOverrideSetters
                ? bin.GetBuilder(typeName)
                    .SetField<object>("code", 99, typeof(byte))
                    .SetField<object>("name", "abc", typeof(string))
                    .Build()
                : bin.GetBuilder(typeName)
                    .SetByteField("code", 99)
                    .SetStringField("name", "abc")
                    .Build();

            var cache = GetBinaryCache();
            cache[1] = obj;
            var res = cache.Get(1);

            Assert.AreEqual("abc", res.GetField<string>("name"));
            Assert.AreEqual(99, res.GetField<byte>("code"));
            Assert.IsNull(res.GetField<object>("field"));

            var type = res.GetBinaryType();
            Assert.AreEqual(typeName, type.TypeName);
            Assert.IsFalse(type.IsEnum);

            CollectionAssert.AreEquivalent(new[] { "code", "name" }, type.Fields);
            Assert.AreEqual("byte", type.GetFieldTypeName("code"));
            Assert.AreEqual("String", type.GetFieldTypeName("name"));

            Assert.AreEqual(type.TypeId, bin.GetBinaryType(typeName).TypeId);
            Assert.AreEqual(type.TypeName, bin.GetBinaryType(type.TypeId).TypeName);
        }

        /// <summary>
        /// Tests the builder with existing class.
        /// </summary>
        [Test]
        public void TestPersonBuilder()
        {
            var fullCache = GetCache<Person>();
            var cache = GetBinaryCache();
            cache[1] = GetBinaryPerson(1);

            // Modify.
            cache[1] = cache[1].ToBuilder().SetField("Name", "Baz").Build();
            Assert.AreEqual("Baz", fullCache[1].Name);

            // Build from scratch.
            cache[2] = Client.GetBinary().GetBuilder(typeof(Person).FullName)
                .SetIntField("Id", 25)
                .SetStringField("Name", "Joe")
                .Build();

            Assert.AreEqual(25, fullCache[2].Id);
            Assert.AreEqual("Joe", fullCache[2].Name);

            // Meta.
            Assert.AreEqual(cache[2].GetBinaryType().TypeId, Client.GetBinary().GetBinaryType(typeof(Person)).TypeId);
        }

        /// <summary>
        /// Tests the enum builder.
        /// </summary>
        [Test]
        public void TestEnumBuilder()
        {
            var bin = Client.GetBinary();
            var cache = GetBinaryCache();

            cache[1] = bin.BuildEnum(typeof(CacheMode), "Replicated");
            Assert.AreEqual((int) CacheMode.Replicated, cache[1].EnumValue);

            Assert.Throws<NotSupportedException>(() => bin.RegisterEnum("MyEnum", new Dictionary<string, int>
            {
                {"Foo", 1},
                {"Bar", 2}
            }));
        }

        /// <summary>
        /// Tests binary types retrieval.
        /// </summary>
        [Test]
        public void TestGetBinaryTypes()
        {
            Assert.Throws<NotSupportedException>(() => Client.GetBinary().GetBinaryTypes());
        }
    }
}
