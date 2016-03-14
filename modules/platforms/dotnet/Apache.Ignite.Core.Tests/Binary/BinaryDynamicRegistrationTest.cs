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
    /// Tests the dynamic type registration.
    /// </summary>
    public class BinaryDynamicRegistrationTest
    {
        [Test]
        public void TestSingleGrid()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Test(ignite, ignite);
            }
        }

        [Test]
        public void TestTwoGrids()
        {
            using (var ignite1 = Ignition.Start(TestUtils.GetTestConfiguration()))
            using (var ignite2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                GridName = "grid2"
            }))
            {
                Test(ignite1, ignite2);
            }
        }

        private static void Test(IIgnite ignite1, IIgnite ignite2)
        {
            const string cacheName = "cache";

            // Put on one grid
            var cache1 = ignite1.CreateCache<int, Foo>(cacheName);
            cache1[1] = new Foo {Int = 1, Str = "1"};

            // Get on another grid
            var cache2 = ignite2.GetCache<int, Foo>(cacheName);
            var foo = cache2[1];

            Assert.AreEqual(1, foo.Int);
            Assert.AreEqual("1", foo.Str);
        }

        private class Foo
        {
            public int Int { get; set; }
            public string Str { get; set; }
        }
    }
}
