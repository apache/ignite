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
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests string serialization.
    /// </summary>
    public class BinaryStringTest
    {
        [Test]
        public void TestNewMode()
        {
            const string springCfg = @"config\compute\compute-grid1.xml";
            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springCfg
            }))
            {
                // TODO: Call Java task with strings

                var res = ignite.GetCompute().Call(new CheckStrings());
                Assert.IsTrue(res);
            }
        }

        [Test]
        public void TestOldMode()
        {
            // Run "TestNewMode" in a separate process
            var envVar = BinaryUtils.IgniteBinaryMarshallerUseStringSerializationVer2;

            Environment.SetEnvironmentVariable(envVar, "false");

            var path = GetType().Assembly.Location;

            var proc = System.Diagnostics.Process.Start(path, GetType().FullName + " Test");

            Assert.IsNotNull(proc);
            Assert.IsTrue(proc.WaitForExit(15000));
            Assert.AreEqual(0, proc.ExitCode);
        }

        [Serializable]
        private class CheckStrings : IComputeFunc<bool>
        {
            public bool Invoke()
            {
                return true;
            }
        }
    }
}
