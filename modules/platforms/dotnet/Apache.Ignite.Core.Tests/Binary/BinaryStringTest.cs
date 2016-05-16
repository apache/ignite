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
    using Apache.Ignite.Core.Tests.Process;
    using NUnit.Framework;

    /// <summary>
    /// Tests string serialization.
    /// </summary>
    public class BinaryStringTest
    {
        // TODO: Test both modes in separate processes
        // Separate process will execute a closure that checks serialization and returns result through environment?

        [Test]
        public void Test([Values(true, false)] bool stringSerVer2)
        {
            Environment.SetEnvironmentVariable(BinaryUtils.IgniteBinaryMarshallerUseStringSerializationVer2,
                stringSerVer2.ToString().ToLowerInvariant());

            const string springCfg = @"config\compute\compute-grid1.xml";
            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springCfg
            }))
            {

                var proc = new IgniteProcess(string.Format("-springConfigUrl={0}", springCfg), "-J-ea",
                    "-J-Xcheck:jni", "-J-Xms512m", "-J-Xmx512m", "-J-DIGNITE_QUIET=false");

                ignite.WaitTopology(2);

                var res = ignite.GetCompute().Call(new CheckStrings());
                Assert.IsTrue(res);

                proc.Kill();
            }
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
