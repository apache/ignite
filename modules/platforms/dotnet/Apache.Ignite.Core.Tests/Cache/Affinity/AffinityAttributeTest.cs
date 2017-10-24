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

#pragma warning disable 169
#pragma warning disable 649
namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using Apache.Ignite.Core.Cache.Affinity;
    using NUnit.Framework;

    /// <summary>
    /// Tests the <see cref="AffinityKeyMappedAttribute"/>.
    /// </summary>
    public class AffinityAttributeTest
    {
        /// <summary>
        /// Tests the property attribute.
        /// </summary>
        [Test]
        public void TestPropertyAttribute()
        {
            Assert.IsNull(AffinityKeyMappedAttribute.GetFieldNameFromAttribute(typeof(NoAttr)));
            Assert.AreEqual("Abc", AffinityKeyMappedAttribute.GetFieldNameFromAttribute(typeof(PublicProperty)));
            Assert.AreEqual("Abc", AffinityKeyMappedAttribute.GetFieldNameFromAttribute(typeof(PrivateProperty)));
        }

        /// <summary>
        /// Tests the field attribute.
        /// </summary>
        [Test]
        public void TestFieldAttribute()
        {
            Assert.AreEqual("Abc", AffinityKeyMappedAttribute.GetFieldNameFromAttribute(typeof(PublicField)));
            Assert.AreEqual("_abc", AffinityKeyMappedAttribute.GetFieldNameFromAttribute(typeof(PrivateField)));
        }

        /// <summary>
        /// Tests multiple attributes per class.
        /// </summary>
        [Test]
        public void TestMultipleAttributes()
        {
            
        }

        /// <summary>
        /// Tests inherited members.
        /// </summary>
        [Test]
        public void TestInheritedMembers()
        {
            
        }

        private class NoAttr
        {
            public string Abc { get; set; }
        }

        private class PublicProperty
        {
            public string Foo { get; set; }

            [AffinityKeyMapped]
            public string Abc { get; set; }
        }

        private class PrivateProperty
        {
            private string Foo { get; set; }

            [AffinityKeyMapped]
            private string Abc { get; set; }
        }

        private class PublicField
        {
            public string Foo;

            [AffinityKeyMapped]
            public string Abc;
        }

        private class PrivateField
        {
            private string _foo;

            [AffinityKeyMapped]
            private string _abc;
        }
    }
}
