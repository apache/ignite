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
    using System.Collections;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests compact footer mode interop with Java.
    /// </summary>
    public class BinaryCompactFooterInteropTest
    {
        /** */
        private const string PlatformSqlQueryTask = "org.apache.ignite.platform.PlatformSqlQueryTask";

        /** */
        private IIgnite _grid;

        /** */
        private IIgnite _clientGrid;

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void TestSetUp()
        {
            // Start fresh cluster for each test
            _grid = Ignition.Start(Config("config\\compute\\compute-grid1.xml"));
            _clientGrid = Ignition.Start(Config("config\\compute\\compute-grid3.xml"));
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TestTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests an object that comes from Java.
        /// </summary>
        [Test]
        public void TestFromJava([Values(true, false)] bool client)
        {
            // Retry multiple times: IGNITE-4377
            for (int i = 0; i < 10; i++)
            {
                var grid = client ? _clientGrid : _grid;

                try
                {
                    var fromJava = grid.GetCompute().ExecuteJavaTask<PlatformComputeBinarizable>(ComputeApiTest.EchoTask,
                        ComputeApiTest.EchoTypeBinarizable);

                    Assert.AreEqual(1, fromJava.Field);

                    return;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("TestFromJava failed on try {0}: \n {1}", i, ex);

                    if (i < 9)
                        continue;
                    
                    throw;
                }
            }
        }

        /// <summary>
        /// Tests an object that comes from .NET in Java.
        /// </summary>
        [Test]
        public void TestFromDotNet([Values(true, false)] bool client)
        {
            var grid = client ? _clientGrid : _grid;

            var compute = grid.GetCompute().WithKeepBinary();

            var arg = new PlatformComputeNetBinarizable {Field = 100};

            var res = compute.ExecuteJavaTask<int>(ComputeApiTest.BinaryArgTask, arg);

            Assert.AreEqual(arg.Field, res);
        }

        /// <summary>
        /// Tests the indexing.
        /// </summary>
        [Test]
        public void TestIndexing([Values(true, false)] bool client)
        {
            var grid = client ? _clientGrid : _grid;

            var cache = grid.GetCache<int, PlatformComputeBinarizable>(null);

            // Populate cache in .NET
            for (var i = 0; i < 100; i++)
                cache[i] = new PlatformComputeBinarizable {Field = i};

            // Run SQL query on Java side
            var qryRes = grid.GetCompute().ExecuteJavaTask<IList>(PlatformSqlQueryTask, "Field < 10");

            Assert.AreEqual(10, qryRes.Count);
            Assert.IsTrue(qryRes.OfType<PlatformComputeBinarizable>().All(x => x.Field < 10));
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
                JvmClasspath = TestUtils.CreateTestClasspath(),
                BinaryConfiguration = new BinaryConfiguration(
                    typeof (PlatformComputeBinarizable),
                    typeof (PlatformComputeNetBinarizable))
            };
        }
    }
}
