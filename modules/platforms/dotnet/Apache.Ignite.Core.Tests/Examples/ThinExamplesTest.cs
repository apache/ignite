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

namespace Apache.Ignite.Core.Tests.Examples
{
    using System.Linq;
    using System.Reflection;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client examples.
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ThinExamplesTest : ExamplesTestBase
    {
        /** */
        public static readonly Example[] ThinExamples = Example.AllExamples.Where(e => e.IsThin).ToArray();

        /** */
        private readonly int _serverCount;

        /// <summary>
        /// Initializes a new instance of <see cref="ThinExamplesTest"/>.
        /// </summary>
        public ThinExamplesTest() : this(1)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ThinExamplesTest"/>.
        /// </summary>
        public ThinExamplesTest(int serverCount)
        {
            _serverCount = serverCount;
        }

        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            for (int i = 0; i < _serverCount; i++)
            {
                Ignition.Start(TestUtils.GetTestConfiguration(name: i.ToString()));
            }

            // Init default services.
            var asmFile = ExamplePaths.GetAssemblyPath(ExamplePaths.SharedProjFile);
            var asm = Assembly.LoadFrom(asmFile);
            var utils = asm.GetType("Apache.Ignite.Examples.Shared.Utils");

            Assert.IsNotNull(utils);

            var ignite = Ignition.GetIgnite("0");

            utils.InvokeMember(
                "DeployDefaultServices",
                BindingFlags.Static | BindingFlags.Public | BindingFlags.InvokeMethod,
                null,
                null,
                new object[] {ignite});
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
        /// Tests the thin client example
        /// </summary>
        [Test, TestCaseSource(nameof(ThinExamples))]
        public void TestThinExample(Example example)
        {
            Assert.IsTrue(example.IsThin);

            example.Run();

            CheckOutput(example);
        }
    }
}
