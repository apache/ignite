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
    public class GridLifecycleTest
    {
        /** Configuration: without Java beans. */
        private const string CFG_NO_BEANS = "config//lifecycle//lifecycle-no-beans.xml";

        /** Configuration: with Java beans. */
        private const string CFG_BEANS = "config//lifecycle//lifecycle-beans.xml";

        /** Whether to throw an error on lifecycle event. */
        internal static bool throwErr;

        /** Events: before start. */
        internal static IList<Event> beforeStartEvts;

        /** Events: after start. */
        internal static IList<Event> afterStartEvts;

        /** Events: before stop. */
        internal static IList<Event> beforeStopEvts;

        /** Events: after stop. */
        internal static IList<Event> afterStopEvts;

        /// <summary>
        /// Set up routine.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            throwErr = false;

            beforeStartEvts = new List<Event>();
            afterStartEvts = new List<Event>();
            beforeStopEvts = new List<Event>();
            afterStopEvts = new List<Event>();
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
            IIgnite grid = Start(CFG_NO_BEANS);

            Assert.AreEqual(2, beforeStartEvts.Count);
            CheckEvent(beforeStartEvts[0], null, null, 0, null);
            CheckEvent(beforeStartEvts[1], null, null, 0, null);

            Assert.AreEqual(2, afterStartEvts.Count);
            CheckEvent(afterStartEvts[0], grid, grid, 0, null);
            CheckEvent(afterStartEvts[1], grid, grid, 0, null);

            // 2. Test stop events.
            Ignition.Stop(grid.Name, false);

            Assert.AreEqual(2, beforeStartEvts.Count);
            Assert.AreEqual(2, afterStartEvts.Count);

            Assert.AreEqual(2, beforeStopEvts.Count);
            CheckEvent(beforeStopEvts[0], grid, grid, 0, null);
            CheckEvent(beforeStopEvts[1], grid, grid, 0, null);

            Assert.AreEqual(2, afterStopEvts.Count);
            CheckEvent(afterStopEvts[0], grid, grid, 0, null);
            CheckEvent(afterStopEvts[1], grid, grid, 0, null);
        }

        /// <summary>
        /// Test with Java beans.
        /// </summary>
        [Test]
        public void TestWithBeans()
        {
            // 1. Test .Net start events.
            IIgnite grid = Start(CFG_BEANS);

            Assert.AreEqual(4, beforeStartEvts.Count);
            CheckEvent(beforeStartEvts[0], null, null, 0, null);
            CheckEvent(beforeStartEvts[1], null, null, 1, "1");
            CheckEvent(beforeStartEvts[2], null, null, 0, null);
            CheckEvent(beforeStartEvts[3], null, null, 0, null);

            Assert.AreEqual(4, afterStartEvts.Count);
            CheckEvent(afterStartEvts[0], grid, grid, 0, null);
            CheckEvent(afterStartEvts[1], grid, grid, 1, "1");
            CheckEvent(afterStartEvts[2], grid, grid, 0, null);
            CheckEvent(afterStartEvts[3], grid, grid, 0, null);

            // 2. Test Java start events.
            IList<int> res = grid.Compute().ExecuteJavaTask<IList<int>>(
                "org.gridgain.interop.lifecycle.GridInteropJavaLifecycleTask", null);

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(3, res[0]);
            Assert.AreEqual(3, res[1]);

            // 3. Test .Net stop events.
            Ignition.Stop(grid.Name, false);

            Assert.AreEqual(4, beforeStartEvts.Count);
            Assert.AreEqual(4, afterStartEvts.Count);

            Assert.AreEqual(4, beforeStopEvts.Count);
            CheckEvent(beforeStopEvts[0], grid, grid, 0, null);
            CheckEvent(beforeStopEvts[1], grid, grid, 1, "1");
            CheckEvent(beforeStopEvts[2], grid, grid, 0, null);
            CheckEvent(beforeStopEvts[3], grid, grid, 0, null);

            Assert.AreEqual(4, afterStopEvts.Count);
            CheckEvent(afterStopEvts[0], grid, grid, 0, null);
            CheckEvent(afterStopEvts[1], grid, grid, 1, "1");
            CheckEvent(afterStopEvts[2], grid, grid, 0, null);
            CheckEvent(afterStopEvts[3], grid, grid, 0, null);
        }

        /// <summary>
        /// Test behavior when error is thrown from lifecycle beans.
        /// </summary>
        [Test]
        public void TestError()
        {
            throwErr = true;

            try
            {
                IIgnite grid = Start(CFG_NO_BEANS);

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
            GridTestUtils.JVM_DEBUG = true;

            GridConfiguration cfg = new GridConfiguration();

            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();
            cfg.JvmOptions = GridTestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = cfgPath;

            cfg.LifecycleBeans = new List<ILifecycleBean>() { new Bean(), new Bean() };

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
            if (evt.grid1 != null && evt.grid1 is GridProxy)
                evt.grid1 = (evt.grid1 as GridProxy).Target;

            if (evt.grid2 != null && evt.grid2 is GridProxy)
                evt.grid2 = (evt.grid2 as GridProxy).Target;

            Assert.AreEqual(expGrid1, evt.grid1);
            Assert.AreEqual(expGrid2, evt.grid2);
            Assert.AreEqual(expProp1, evt.prop1);
            Assert.AreEqual(expProp2, evt.prop2);
        }
    }

    public abstract class AbstractBean
    {
        [InstanceResource]
        public IIgnite grid1;

        public int Property1
        {
            get;
            set;
        }
    }

    public class Bean : AbstractBean, ILifecycleBean
    {
        [InstanceResource]
        public IIgnite grid2;

        public string Property2
        {
            get;
            set;
        }

        /** <inheritDoc /> */
        public void OnLifecycleEvent(LifecycleEventType evtType)
        {
            if (GridLifecycleTest.throwErr)
                throw new Exception("Lifecycle exception.");

            Event evt = new Event();

            evt.grid1 = grid1;
            evt.grid2 = grid2;
            evt.prop1 = Property1;
            evt.prop2 = Property2;

            switch (evtType)
            {
                case LifecycleEventType.BEFORE_GRID_START:
                    GridLifecycleTest.beforeStartEvts.Add(evt);

                    break;

                case LifecycleEventType.AFTER_GRID_START:
                    GridLifecycleTest.afterStartEvts.Add(evt);

                    break;

                case LifecycleEventType.BEFORE_GRID_STOP:
                    GridLifecycleTest.beforeStopEvts.Add(evt);

                    break;

                case LifecycleEventType.AFTER_GRID_STOP:
                    GridLifecycleTest.afterStopEvts.Add(evt);

                    break;
            }
        }
    }

    public class Event
    {
        public IIgnite grid1;
        public IIgnite grid2;
        public int prop1;
        public string prop2;
    }
}
