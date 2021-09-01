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

namespace Apache.Ignite.Core.Tests.Compute
{
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using NUnit.Framework;
    using Apache.Ignite.Platform.Model;

    /// <summary>
    /// Compute tests for type auto register with Java tasks.
    /// </summary>
    public class ComputeApiTypeAutoRegisterTest
    {
        /** Echo type: V1. */
        private const int EchoTypeV1Action = 24;

        /** Echo type: V3. */
        private const int EchoTypeV3Action = 25;

        /** First node. */
        private IIgnite _grid1;

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitGrid()
        {
            _grid1 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = Path.Combine("Config", "Compute", "compute-grid") + "1.xml",
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new BinaryBasicNameMapper {NamespacePrefix = "org.", NamespaceToLower = true}
                }
            });
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that types will be auto register on task invocation.
        /// </summary>
        [Test]
        public void TestEchoTasksAutoRegisterType()
        {
            var v1 = _grid1.GetCompute().ExecuteJavaTask<V1>(ComputeApiTest.EchoTask, EchoTypeV1Action);

            Assert.AreEqual("V1", v1.Name);

            var arg = new V2 {Name = "V2"};

            var v2 = _grid1.GetCompute().ExecuteJavaTask<V2>(ComputeApiTest.EchoArgTask, arg);

            Assert.AreEqual(arg.Name, v2.Name);
        }

        /// <summary>
        /// Tests that types will be auto register on task async invocation.
        /// </summary>
        [Test]
        public void TestEchoAsyncTasksAutoRegisterType()
        {
            var v3 = _grid1.GetCompute().ExecuteJavaTaskAsync<V3>(ComputeApiTest.EchoTask, EchoTypeV3Action).Result;

            Assert.AreEqual("V3", v3.Name);

            var arg = new V4 {Name = "V4"};

            var v4 = _grid1.GetCompute().ExecuteJavaTaskAsync<V4>(ComputeApiTest.EchoArgTask, arg).Result;

            Assert.AreEqual(arg.Name, v4.Name);
        }
    }
}
