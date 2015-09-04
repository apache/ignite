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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Lifecycle beans test.
    /// </summary>
    public class LifecycleTest
    {
        /** Configuration: without Java beans. */
        private const string CfgNoBeans = "config//lifecycle//lifecycle-no-beans.xml";

        /** Configuration: with Java beans. */
        private const string CfgBeans = "config//lifecycle//lifecycle-beans.xml";

        /** Whether to throw an error on lifecycle event. */
        internal static bool ThrowErr;

        /** Events: before start. */
        internal static IList<Event> BeforeStartEvts;

        /** Events: after start. */
        internal static IList<Event> AfterStartEvts;

        /** Events: before stop. */
        internal static IList<Event> BeforeStopEvts;

        /** Events: after stop. */
        internal static IList<Event> AfterStopEvts;

        /// <summary>
        /// Set up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            ThrowErr = false;

            BeforeStartEvts = new List<Event>();
            AfterStartEvts = new List<Event>();
            BeforeStopEvts = new List<Event>();
            AfterStopEvts = new List<Event>();
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }
        
        /// <summary>
        /// Test without Java beans.
        /// </summary>
        [Test]
        public void TestWithoutBeans()
        {
            // 1. Test start events.
            IIgnite grid = Start(CfgNoBeans);

            Assert.AreEqual(2, BeforeStartEvts.Count);
            CheckEvent(BeforeStartEvts[0], null, null, 0, null);
            CheckEvent(BeforeStartEvts[1], null, null, 0, null);

            Assert.AreEqual(2, AfterStartEvts.Count);
            CheckEvent(AfterStartEvts[0], grid, grid, 0, null);
            CheckEvent(AfterStartEvts[1], grid, grid, 0, null);

            // 2. Test stop events.
            Ignition.Stop(grid.Name, false);

            Assert.AreEqual(2, BeforeStartEvts.Count);
            Assert.AreEqual(2, AfterStartEvts.Count);

            Assert.AreEqual(2, BeforeStopEvts.Count);
            CheckEvent(BeforeStopEvts[0], grid, grid, 0, null);
            CheckEvent(BeforeStopEvts[1], grid, grid, 0, null);

            Assert.AreEqual(2, AfterStopEvts.Count);
            CheckEvent(AfterStopEvts[0], grid, grid, 0, null);
            CheckEvent(AfterStopEvts[1], grid, grid, 0, null);
        }

        /// <summary>
        /// Test with Java beans.
        /// </summary>
        [Test]
        public void TestWithBeans()
        {
            // 1. Test .Net start events.
            IIgnite grid = Start(CfgBeans);

            Assert.AreEqual(4, BeforeStartEvts.Count);
            CheckEvent(BeforeStartEvts[0], null, null, 0, null);
            CheckEvent(BeforeStartEvts[1], null, null, 1, "1");
            CheckEvent(BeforeStartEvts[2], null, null, 0, null);
            CheckEvent(BeforeStartEvts[3], null, null, 0, null);

            Assert.AreEqual(4, AfterStartEvts.Count);
            CheckEvent(AfterStartEvts[0], grid, grid, 0, null);
            CheckEvent(AfterStartEvts[1], grid, grid, 1, "1");
            CheckEvent(AfterStartEvts[2], grid, grid, 0, null);
            CheckEvent(AfterStartEvts[3], grid, grid, 0, null);

            // 2. Test Java start events.
            IList<int> res = grid.Compute().ExecuteJavaTask<IList<int>>(
                "org.apache.ignite.platform.lifecycle.PlatformJavaLifecycleTask", null);

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(3, res[0]);
            Assert.AreEqual(3, res[1]);

            // 3. Test .Net stop events.
            Ignition.Stop(grid.Name, false);

            Assert.AreEqual(4, BeforeStartEvts.Count);
            Assert.AreEqual(4, AfterStartEvts.Count);

            Assert.AreEqual(4, BeforeStopEvts.Count);
            CheckEvent(BeforeStopEvts[0], grid, grid, 0, null);
            CheckEvent(BeforeStopEvts[1], grid, grid, 1, "1");
            CheckEvent(BeforeStopEvts[2], grid, grid, 0, null);
            CheckEvent(BeforeStopEvts[3], grid, grid, 0, null);

            Assert.AreEqual(4, AfterStopEvts.Count);
            CheckEvent(AfterStopEvts[0], grid, grid, 0, null);
            CheckEvent(AfterStopEvts[1], grid, grid, 1, "1");
            CheckEvent(AfterStopEvts[2], grid, grid, 0, null);
            CheckEvent(AfterStopEvts[3], grid, grid, 0, null);
        }

        /// <summary>
        /// Test behavior when error is thrown from lifecycle beans.
        /// </summary>
        [Test]
        public void TestError()
        {
            ThrowErr = true;

            try
            {
                Start(CfgNoBeans);

                Assert.Fail("Should not reach this place.");
            }
            catch (Exception e)
            {
                Assert.AreEqual(typeof(IgniteException), e.GetType());
            }
        }

        /// <summary>
        /// Start grid.
        /// </summary>
        /// <param name="cfgPath">Spring configuration path.</param>
        /// <returns>Grid.</returns>
        private static IIgnite Start(string cfgPath)
        {
            TestUtils.JvmDebug = true;

            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.JvmClasspath = TestUtils.CreateTestClasspath();
            cfg.JvmOptions = TestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = cfgPath;

            cfg.LifecycleBeans = new List<ILifecycleBean> { new Bean(), new Bean() };

            return Ignition.Start(cfg);
        }

        /// <summary>
        /// Check event.
        /// </summary>
        /// <param name="evt">Event.</param>
        /// <param name="expGrid1">Expected grid 1.</param>
        /// <param name="expGrid2">Expected grid 2.</param>
        /// <param name="expProp1">Expected property 1.</param>
        /// <param name="expProp2">Expected property 2.</param>
        private static void CheckEvent(Event evt, IIgnite expGrid1, IIgnite expGrid2, int expProp1, string expProp2)
        {
            if (evt.Grid1 != null && evt.Grid1 is IgniteProxy)
                evt.Grid1 = (evt.Grid1 as IgniteProxy).Target;

            if (evt.Grid2 != null && evt.Grid2 is IgniteProxy)
                evt.Grid2 = (evt.Grid2 as IgniteProxy).Target;

            Assert.AreEqual(expGrid1, evt.Grid1);
            Assert.AreEqual(expGrid2, evt.Grid2);
            Assert.AreEqual(expProp1, evt.Prop1);
            Assert.AreEqual(expProp2, evt.Prop2);
        }
    }

    public abstract class AbstractBean
    {
        [InstanceResource]
        public IIgnite Grid1;

        public int Property1
        {
            get;
            set;
        }
    }

    public class Bean : AbstractBean, ILifecycleBean
    {
        [InstanceResource]
        public IIgnite Grid2;

        public string Property2
        {
            get;
            set;
        }

        /** <inheritDoc /> */
        public void OnLifecycleEvent(LifecycleEventType evtType)
        {
            if (LifecycleTest.ThrowErr)
                throw new Exception("Lifecycle exception.");

            Event evt = new Event();

            evt.Grid1 = Grid1;
            evt.Grid2 = Grid2;
            evt.Prop1 = Property1;
            evt.Prop2 = Property2;

            switch (evtType)
            {
                case LifecycleEventType.BeforeNodeStart:
                    LifecycleTest.BeforeStartEvts.Add(evt);

                    break;

                case LifecycleEventType.AfterNodeStart:
                    LifecycleTest.AfterStartEvts.Add(evt);

                    break;

                case LifecycleEventType.BeforeNodeStop:
                    LifecycleTest.BeforeStopEvts.Add(evt);

                    break;

                case LifecycleEventType.AfterNodeStop:
                    LifecycleTest.AfterStopEvts.Add(evt);

                    break;
            }
        }
    }

    public class Event
    {
        public IIgnite Grid1;
        public IIgnite Grid2;
        public int Prop1;
        public string Prop2;
    }
}
