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

namespace Apache.Ignite.Core.Tests
{
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Test marshaller initialization.
    /// </summary>
    public class MarshallerTest
    {
        /// <summary>
        /// Tests the default marhsaller.
        /// By default, binary marshaller is used.
        /// </summary>
        [Test]
        public void TestDefaultMarhsaller()
        {
            using (var grid = StartIgnite("Config\\marshaller-default.xml"))
            {
                var cache = grid.GetOrCreateCache<int, int>("default");

                cache.Put(1, 1);

                Assert.AreEqual(1, cache.Get(1));
            }
        }

        /// <summary>
        /// Tests the binary marshaller.
        /// Marshaller can be specified explicitly in config.
        /// </summary>
        [Test]
        public void TestExplicitMarshaller()
        {
            using (var grid = StartIgnite("Config\\marshaller-explicit.xml"))
            {
                var cache = grid.GetOrCreateCache<int, int>("default");

                cache.Put(1, 1);

                Assert.AreEqual(1, cache.Get(1));
            }
        }

        /// <summary>
        /// Tests the invalid marshaller.
        /// </summary>
        [Test]
        public void TestInvalidMarshaller()
        {
            var ex = Assert.Throws<IgniteException>(() => StartIgnite("Config\\marshaller-invalid.xml"));
            Assert.AreEqual("Unsupported marshaller (only org.apache.ignite.internal.binary.BinaryMarshaller " +
                            "can be used when running Apache Ignite.NET): org.apache.ignite.internal." +
                            "marshaller.optimized.OptimizedMarshaller", ex.Message, ex.ToString());
        }

        /// <summary>
        /// Starts the ignite.
        /// </summary>
        private static IIgnite StartIgnite(string xml)
        {
            return Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = xml
            });
        }
    }
}
