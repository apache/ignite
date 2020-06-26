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

namespace Apache.Ignite.Core.Tests
{
    using NUnit.Framework;

    /// <summary>
    /// Code configuration test base.
    /// </summary>
    public abstract class TestBase
    {
        /** */
        private readonly int _gridCount;

        /// <summary>
        /// Initializes a new instance of <see cref="TestBase"/> class.
        /// </summary>
        protected TestBase(int gridCount = 1)
        {
            _gridCount = gridCount;
        }

        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Ignition.Start(GetConfig());

            for (var i = 1; i < _gridCount; i++)
            {
                var cfg = new IgniteConfiguration(GetConfig())
                {
                    IgniteInstanceName = i.ToString()
                };

                Ignition.Start(cfg);
            }
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

        /// <summary>
        /// Gets the second Ignite instance, if present.
        /// </summary>
        protected IIgnite Ignite2
        {
            get { return Ignition.GetIgnite("1"); }
        }
    }
}