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
    using NUnit.Framework;

    /// <summary>
    /// Enums test with Ignite running.
    /// </summary>
    public class EnumsTestOnline : EnumsTest
    {
        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
        }

        /// <summary>
        /// Tears down the fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }
    }

    // TODO
    public class EnumsTest2
    {
        enum TestEnum
        {
            TestValue1,
            TestValue2
        };

        class TestClass
        {
            public string Name { get; set; }
            public TestEnum? EnumValue { get; set; }

            public TestClass(string name, TestEnum? enumValue)
            {
                Name = name;
                EnumValue = enumValue;
            }
        }

        [Test]
        public void TestMe()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                var cache = ignite.CreateCache<string, TestClass>("c");
                cache.Put("TestElem1", new TestClass("TestElem1", TestEnum.TestValue1));
                var res = cache.Get("TestElem1");
                Assert.AreEqual(TestEnum.TestValue1, res.EnumValue);
            }
        }
    }
}