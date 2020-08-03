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
    using System.Collections.Generic;
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="DynamicFieldSetSerializable"/> serialization.
    /// </summary>
    public class DynamicFieldSetTest
    {
        /** */
        private IIgnite _node1;
        
        /** */
        private IIgnite _node2;
        
        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _node1 = Ignition.Start(TestUtils.GetTestConfiguration());
            
            _node2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                AutoGenerateIgniteInstanceName = true
            });
        }
        
        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }
        
        /// <summary>
        /// Tests that dynamically added and removed fields are deserialized correctly.
        /// This verifies proper metadata and schema handling.
        /// </summary>
        [Test]
        public void TestAddRemoveFieldsDynamically()
        {
            var cache1 = _node1.CreateCache<int, DynamicFieldSetSerializable>("c");
            var cache2 = _node2.GetCache<int, DynamicFieldSetSerializable>("c");
            
            // Put/get without optional fields.
            var noFields = new DynamicFieldSetSerializable();
            cache1[1] = noFields;
            
            AssertExtensions.ReflectionEqual(noFields, cache1[1]);
            AssertExtensions.ReflectionEqual(noFields, cache2[1]);

            Assert.AreEqual(new[] {"WriteBar", "WriteFoo"}, GetFields(0));
            Assert.AreEqual(new[] {"WriteBar", "WriteFoo"}, GetFields(1));
            
            // Put/get with one optional field.
            var oneField = new DynamicFieldSetSerializable
            {
                Bar = "abc",
                WriteBar = true
            };
            cache1[2] = oneField;
            
            AssertExtensions.ReflectionEqual(oneField, cache1[2]);
            AssertExtensions.ReflectionEqual(oneField, cache2[2]);
            Assert.AreEqual(new[] {"Bar", "WriteBar", "WriteFoo"}, GetFields(0));
            Assert.AreEqual(new[] {"Bar", "WriteBar", "WriteFoo"}, GetFields(1));
            
            // Put/get with another optional field.
            var oneField2 = new DynamicFieldSetSerializable
            {
                Foo = 25,
                WriteFoo = true
            };
            cache1[3] = oneField2;
            
            AssertExtensions.ReflectionEqual(oneField2, cache1[3]);
            AssertExtensions.ReflectionEqual(oneField2, cache2[3]);
            
            Assert.AreEqual(new[] {"Bar", "Foo", "WriteBar", "WriteFoo"}, GetFields(0));
            Assert.AreEqual(new[] {"Bar", "Foo", "WriteBar", "WriteFoo"}, GetFields(1));
        }

        /// <summary>
        /// Gets the fields.
        /// </summary>
        private IEnumerable<string> GetFields(int nodeIndex)
        {
            var node = nodeIndex == 0 ? _node1 : _node2;
            
            return node
                .GetBinary()
                .GetBinaryType(typeof(DynamicFieldSetSerializable))
                .Fields
                .OrderBy(f => f);
        }
    }
}