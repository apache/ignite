/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
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
            Environment.SetEnvironmentVariable(BinaryUtils.IgniteBinaryMarshallerUseStringSerializationVer2, "true");

            TestUtils.RunTestInNewProcess(GetType().FullName, "TestOldMode");
        }

        /// <summary>
        /// Test tear down.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Environment.SetEnvironmentVariable(BinaryUtils.IgniteBinaryMarshallerUseStringSerializationVer2, null);
        }
    }
}
