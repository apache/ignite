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
    /// Tests string serialization.
    /// </summary>
    public class BinaryStringTest
    {
        /** */
        private const string StringTestTask = "org.apache.ignite.platform.PlatformStringTestTask";

        /// <summary>
        /// Tests the default mode.
        /// </summary>
        [Test]
        public void TestDefaultMode()
        {
            Assert.IsFalse(BinaryUtils.UseStringSerializationVer2);
        }

        /// <summary>
        /// Tests the new serialization mode.
        /// </summary>
        [Test]
        public void TestOldMode()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration(false)))
            {
                CheckString(ignite, "Normal string строка 123 — ☺");

                if (BinaryUtils.UseStringSerializationVer2)
                {
                    foreach (var specialString in BinarySelfTest.SpecialStrings)
                        CheckString(ignite, specialString);
                }
                else
                {
                    CheckString(ignite, BinarySelfTest.SpecialStrings[0], true);
                }
            }
        }

        /// <summary>
        /// Checks the string.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        /// <param name="test">The test string.</param>
        /// <param name="fail">Whether the check should fail.</param>
        private static void CheckString(IIgnite ignite, string test, bool fail = false)
        {
            var res = ignite.GetCompute().ExecuteJavaTask<string>(StringTestTask, test);

            if (fail)
                Assert.AreNotEqual(test, res);
            else
                Assert.AreEqual(test, res);
        }

        /// <summary>
        /// Tests the old serialization mode.
        /// </summary>
        [Test]
        public void TestNewMode()
        {
            // Run "TestOldMode" in a separate process with changed setting.
            using (EnvVar.Set(BinaryUtils.IgniteBinaryMarshallerUseStringSerializationVer2, "true"))
            {
                TestUtils.RunTestInNewProcess(GetType().FullName, "TestOldMode");
            }
        }
    }
}
