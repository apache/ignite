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
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests type registration with BinaryBuilder.
    /// </summary>
    public class BinaryBuilderTypeRegistrationTest
    {
        /// <summary>
        /// Sets up the test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
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

        /// <summary>
        /// Tests the class registration.
        /// </summary>
        [Test]
        public void TestClassRegistration()
        {
            var bin = Ignition.GetIgnite().GetBinary();

            var binType = bin.GetBinaryType(typeof(Bar));
            Assert.AreEqual(typeof(Bar).FullName, binType.TypeName);
            Assert.IsFalse(binType.IsEnum);
            Assert.AreEqual(BinaryUtils.GetStringHashCode(typeof(Bar).FullName), binType.TypeId);
        }

        /// <summary>
        /// Tests the classless registration (type name only, no corresponding type).
        /// </summary>
        [Test]
        public void TestClasslessRegistration()
        {
            var bin = Ignition.GetIgnite().GetBinary();

            var binType = bin.GetBinaryType("foo");
            Assert.AreEqual("foo", binType.TypeName);
            Assert.IsFalse(binType.IsEnum);
            Assert.AreEqual(BinaryUtils.GetStringHashCode("foo"), binType.TypeId);
        }

        /// <summary>
        /// Test class.
        /// </summary>
        private class Bar
        {
            // No-op.
        }
    }
}
