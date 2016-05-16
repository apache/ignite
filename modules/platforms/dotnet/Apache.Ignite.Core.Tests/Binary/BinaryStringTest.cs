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
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests string serialization.
    /// </summary>
    public class BinaryStringTest
    {
        /** */
        const string StringTestTask = "org.apache.ignite.platform.PlatformStringTestTask";

        /// <summary>
        /// Tests the default mode.
        /// </summary>
        [Test]
        public void TestDefaultMode()
        {
            Assert.IsTrue(BinaryUtils.UseStringSerializationVer2);
        }

        /// <summary>
        /// Tests the new serialization mode.
        /// </summary>
        [Test]
        public void TestNewMode()
        {
            TestUtils.JvmDebug = false;  // avoid conflicting listener from standalone process

            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                CheckString(ignite, "Normal string");

                if (BinaryUtils.UseStringSerializationVer2)
                {
                    foreach (var specialString in BinarySelfTest.SpecialStrings)
                        CheckString(ignite, specialString);
                }
            }
        }

        /// <summary>
        /// Checks the string.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        /// <param name="test">The test string.</param>
        private static void CheckString(IIgnite ignite, string test)
        {
            Assert.AreEqual(test, ignite.GetCompute().ExecuteJavaTask<string>(StringTestTask, test));
        }

        /// <summary>
        /// Tests the old serialization mode.
        /// </summary>
        [Test]
        public void TestOldMode()
        {
            // Run "TestNewMode" in a separate process
            var envVar = BinaryUtils.IgniteBinaryMarshallerUseStringSerializationVer2;

            Environment.SetEnvironmentVariable(envVar, "false");

            var procStart = new ProcessStartInfo
            {
                FileName = GetType().Assembly.Location,
                Arguments = GetType().FullName + " TestNewMode",
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            var proc = Process.Start(procStart);

            Assert.IsNotNull(proc);

            Console.WriteLine(proc.StandardOutput.ReadToEnd());
            Console.WriteLine(proc.StandardError.ReadToEnd());
            Assert.IsTrue(proc.WaitForExit(15000));
            Assert.AreEqual(0, proc.ExitCode);
        }
    }
}
