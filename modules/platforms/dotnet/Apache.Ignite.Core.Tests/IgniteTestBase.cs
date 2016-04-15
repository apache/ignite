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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Messaging;
    using NUnit.Framework;

    /// <summary>
    /// Base class for all grid tests.
    /// </summary>
    [Serializable]
    public abstract class IgniteTestBase
    {
        /** Grids. */
        [NonSerialized]
        private IIgnite[] _grids;

        /** Config urls. */
        [NonSerialized]
        private readonly string[] _springUrls;

        /** Expected entry count by the end of the test. */
        [NonSerialized]
        private readonly int _expectedHandleRegistryEntries;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteTestBase"/> class.
        /// </summary>
        /// <param name="springUrls">The spring urls.</param>
        protected IgniteTestBase(params string[] springUrls)
            : this(0, springUrls)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteTestBase"/> class.
        /// </summary>
        /// <param name="springUrls">The spring urls.</param>
        /// <param name="expectedHandleRegistryEntries">The expected handle registry entries.</param>
        protected IgniteTestBase(int expectedHandleRegistryEntries, params string[] springUrls)
        {
            _springUrls = springUrls.ToArray();

            _grids = new IIgnite[_springUrls.Length];

            Assert.IsTrue(_grids.Length > 0);

            _expectedHandleRegistryEntries = expectedHandleRegistryEntries;
        }

        /// <summary>
        /// Gets the grid1.
        /// </summary>
        public IIgnite Grid
        {
            get { return _grids[0]; }
        }

        /// <summary>
        /// Gets the grid2.
        /// </summary>
        public IIgnite Grid2
        {
            get { return _grids[1]; }
        }

        /// <summary>
        /// Gets the grid3.
        /// </summary>
        public IIgnite Grid3
        {
            get { return _grids[2]; }
        }

        /// <summary>
        /// Gets the events.
        /// </summary>
        public IEvents Events
        {
            get { return Grid.GetEvents(); }
        }

        /// <summary>
        /// Gets the messaging.
        /// </summary>
        public IMessaging Messaging
        {
            get { return Grid.GetMessaging(); }
        }

        /// <summary>
        /// Gets the compute.
        /// </summary>
        public ICompute Compute
        {
            get { return Grid.GetCompute(); }
        }

        /// <summary>
        /// Gets the grids.
        /// </summary>
        public ICollection<IIgnite> Grids
        {
            get { return _grids; }
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void FixtureTearDown()
        {
            StopGrids();
        }

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public virtual void TestSetUp()
        {
            StartGrids();
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public virtual void TestTearDown()
        {
            try
            {
                TestUtils.AssertHandleRegistryHasItems(1000, _expectedHandleRegistryEntries, _grids);
            }
            catch (Exception)
            {
                // Restart grids to cleanup
                StopGrids();

                throw;
            }
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (Grid != null)
                return;

            _grids = _springUrls.Select(x => Ignition.Start(GetConfiguration(x))).ToArray();
        }

        /// <summary>
        /// Gets the grid configuration.
        /// </summary>
        protected virtual IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            return new IgniteConfiguration
            {
                SpringConfigUrl = springConfigUrl,
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            for (var i = 0; i < _grids.Length; i++)
                _grids[i] = null;

            Ignition.StopAll(true);
        }
    }
}
