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

#pragma warning disable 618
namespace Apache.Ignite.Core.Tests.Binary
{
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests compact footer mode interop with Java.
    /// </summary>
    public class BinaryCompactFooterInteropTest
    {
        /** */
        private IIgnite _grid1;

        /** */
        //private IIgnite _grid2;
        
        /** */
        private IIgnite _clientGrid;

        [SetUp]
        public void TestSetUp()
        {
            // Start fresh cluster for each test
            _grid1 = Ignition.Start(Config("config\\compute\\compute-grid1.xml"));
            //_grid2 = Ignition.Start(Config("config\\compute\\compute-grid2.xml"));
            _clientGrid = Ignition.Start(Config("config\\compute\\compute-grid3.xml"));
        }

        [TearDown]
        public void TestTearDown()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public void TestFromJava([Values(true, false)] bool client)
        {
            // TODO: use compute-grid xmls
            // Add ECHO_TYPE_FROM_CACHE 
            var grid = client ? _clientGrid : _grid1;

            var fromJava = grid.GetCompute().ExecuteJavaTask<PlatformComputeBinarizable>(ComputeApiTest.EchoTask, 
                ComputeApiTest.EchoTypeBinarizable);

            Assert.AreEqual(1, fromJava.Field);
        }

        /// <summary>
        /// Gets the config.
        /// </summary>
        private static IgniteConfiguration Config(string springUrl)
        {
            return new IgniteConfiguration
            {
                SpringConfigUrl = springUrl,
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath()
            };
        }
    }
}
