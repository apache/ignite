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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests.NuGet
{
    using System;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Compute test.
    /// </summary>
    public class ComputeTest
    {
        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
            });
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests cache put/get.
        /// </summary>
        [Test]
        public void TestCompute()
        {
            var ignite = Ignition.GetIgnite();

            var compute = ignite.GetCompute();

            ComputeAction.RunCount = 0;

            compute.Broadcast(new ComputeAction());

            Assert.AreEqual(1, ComputeAction.RunCount);
        }

        /// <summary>
        /// Test action.
        /// </summary>
        [Serializable]
        private class ComputeAction : IComputeAction
        {
            /// <summary> The run count. </summary>
            public static volatile int RunCount;

            /** <inheritdoc /> */
            public void Invoke()
            {
                RunCount++;
            }
        }
    }
}
