/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests
{
    using NUnit.Framework;

    /// <summary>
    /// Code configuration test base.
    /// </summary>
    public class TestBase
    {
        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Ignition.Start(GetConfig());
        }

        /// <summary>
        /// Tears down the fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        protected virtual IgniteConfiguration GetConfig()
        {
            return TestUtils.GetTestConfiguration();
        }

        /// <summary>
        /// Gets an Ignite instance.
        /// </summary>
        protected IIgnite Ignite
        {
            get { return Ignition.GetIgnite(); }
        }
    }
}