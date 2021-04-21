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
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Tests.Binary.Serializable;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="DynamicFieldSetSerializable"/> serialization in thin client.
    /// </summary>
    [TestFixture(true)]
    [TestFixture(false)]
    public class DynamicFieldSetTest : ClientTestBase
    {
        /** */
        private readonly bool _clientToServer;

        /// <summary>
        /// Initializes a new instance of <see cref="DynamicFieldSetTest"/>. 
        /// </summary>
        public DynamicFieldSetTest(bool clientToServer)
        {
            _clientToServer = clientToServer;
        }

        /// <summary>
        /// Tests that dynamically added and removed fields are deserialized correctly.
        /// This verifies proper metadata and schema handling.
        /// </summary>
        [Test]
        public void TestAddRemoveFieldsDynamically()
        {
            var cache1 = Ignition.GetIgnite().GetOrCreateCache<int, DynamicFieldSetSerializable>("c").AsCacheClient();
            var cache2 = Client.GetCache<int, DynamicFieldSetSerializable>("c");

            if (_clientToServer)
            {
                // Swap caches to verify that metadata propagates both ways.
                var tmp = cache1;
                cache1 = cache2;
                cache2 = tmp;
            }

            // Put/get without optional fields.
            var noFields = new DynamicFieldSetSerializable();
            cache1[1] = noFields;

            AssertExtensions.ReflectionEqual(noFields, cache1[1]);
            AssertExtensions.ReflectionEqual(noFields, cache2[1]);

            Assert.AreEqual(new[] {"WriteBar", "WriteFoo"}, GetFieldsServer());
            Assert.AreEqual(new[] {"WriteBar", "WriteFoo"}, GetFieldsClient());

            // Put/get with one optional field.
            var oneField = new DynamicFieldSetSerializable
            {
                Bar = "abc",
                WriteBar = true
            };
            cache1[2] = oneField;

            AssertExtensions.ReflectionEqual(oneField, cache1[2]);
            AssertExtensions.ReflectionEqual(oneField, cache2[2]);
            Assert.AreEqual(new[] {"Bar", "WriteBar", "WriteFoo"}, GetFieldsServer());
            Assert.AreEqual(new[] {"Bar", "WriteBar", "WriteFoo"}, GetFieldsClient());

            // Put/get with another optional field.
            var oneField2 = new DynamicFieldSetSerializable
            {
                Foo = 25,
                WriteFoo = true
            };
            cache1[3] = oneField2;

            AssertExtensions.ReflectionEqual(oneField2, cache1[3]);
            AssertExtensions.ReflectionEqual(oneField2, cache2[3]);

            Assert.AreEqual(new[] {"Bar", "Foo", "WriteBar", "WriteFoo"}, GetFieldsServer());
            Assert.AreEqual(new[] {"Bar", "Foo", "WriteBar", "WriteFoo"}, GetFieldsClient());
            
            // Put/get with both optional fields.
            var twoField = new DynamicFieldSetSerializable
            {
                Bar = "x",
                Foo = 42,
                WriteBar = true,
                WriteFoo = true
            };
            cache1[4] = twoField;

            AssertExtensions.ReflectionEqual(twoField, cache1[4]);
            AssertExtensions.ReflectionEqual(twoField, cache2[4]);
            
            // Re-check initial object without optional fields.
            AssertExtensions.ReflectionEqual(noFields, cache1[1]);
            AssertExtensions.ReflectionEqual(noFields, cache2[1]);
        }

        /// <summary>
        /// Gets the fields.
        /// </summary>
        private IEnumerable<string> GetFieldsServer()
        {
            return Ignition
                .GetIgnite()
                .GetBinary()
                .GetBinaryType(typeof(DynamicFieldSetSerializable))
                .Fields
                .OrderBy(f => f);
        }

        /// <summary>
        /// Gets the fields.
        /// </summary>
        private IEnumerable<string> GetFieldsClient()
        {
            return Client
                .GetBinary()
                .GetBinaryType(typeof(DynamicFieldSetSerializable))
                .Fields
                .OrderBy(f => f);
        }
    }
}