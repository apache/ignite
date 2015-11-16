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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Events;

    using NUnit.Framework;

    /// <summary>
    /// <see cref="EventsExtensions"/> tests.
    /// </summary>
    [Serializable]
    public class EventsExtensionsTest : IgniteTestBase
    {
        /** */
        private static readonly int EvtType = EventType.JobStarted;

        /** */
        private int _testField = 1;

        /// <summary>
        /// Initializes a new instance of the <see cref="EventsExtensionsTest"/> class.
        /// </summary>
        public EventsExtensionsTest() 
            : base("config\\compute\\compute-grid1.xml", "config\\compute\\compute-grid2.xml")
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void TestSetUp()
        {
            base.TestSetUp();

            foreach (var grid in Grids)
                grid.GetEvents().EnableLocal(EvtType);
        }

        /// <summary>
        /// Tests local listen.
        /// </summary>
        [Test]
        public void TestLocalListen()
        {
            int eventCount = 0;

            var listener = Events.LocalListen<IEvent>(evt => ++eventCount + _testField > 0, EvtType);

            GenerateTaskEvent();

            Assert.IsTrue(TestUtils.WaitForCondition(() => 1 == eventCount, 500));

            Events.StopLocalListen(listener, EvtType);
        }

        /// <summary>
        /// Tests WaitForLocal.
        /// </summary>
        [Test]
        public void TestWaitForLocal()
        {
            Task.Factory.StartNew(() =>
            {
                Thread.Sleep(100);
                GenerateTaskEvent();
            });

            int evtType = EvtType;  // test variable capture

            var evt = Events.WaitForLocal<JobEvent>(e => e.Type == evtType, EvtType);

            Assert.IsNotNull(evt);
        }

        /// <summary>
        /// Tests WaitForLocal.
        /// </summary>
        [Test]
        public void TestWaitForLocalAsync()
        {
            Task.Factory.StartNew(() =>
            {
                Thread.Sleep(100);
                GenerateTaskEvent();
            });

            int evtType = EvtType;  // test variable capture

            var evt = Events.WaitForLocalAsync<JobEvent>(e => e.Type == evtType, EvtType).Result;

            Assert.IsNotNull(evt);
        }

        /// <summary>
        /// Tests RemoteQuery.
        /// </summary>
        [Test]
        public void TestRemoteQuery()
        {
            GenerateTaskEvent();

            int evtType = EvtType;  // test variable capture

            var events = Events.RemoteQuery<IEvent>(e => e.Type == evtType);

            Assert.IsTrue(events.Cast<JobEvent>().Any());

            events = Events.RemoteQueryAsync<IEvent>(e => e.Type == evtType).Result;

            Assert.IsTrue(events.Cast<JobEvent>().Any());
        }

        /// <summary>
        /// Generates the task event.
        /// </summary>
        private void GenerateTaskEvent()
        {
            Compute.Broadcast(() => 0);
        }
    }
}
