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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Events;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// <see cref="IEvents"/> tests.
    /// </summary>
    public class GridEventsTest
    {
        /** */
        private IIgnite grid1;

        /** */
        private IIgnite grid2;

        /** */
        private IIgnite grid3;

        /** */
        private IIgnite[] grids;
        
        /** */
        public static int idGen;

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            StopGrids();
        }

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();
            EventsTestHelper.listenResult = true;
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public virtual void TearDown()
        {
            try
            {
                GridTestUtils.AssertHandleRegistryIsEmpty(1000, grid1, grid2, grid3);
            }
            catch (Exception)
            {
                // Restart grids to cleanup
                StopGrids();

                throw;
            }
            finally
            {
                EventsTestHelper.AssertFailures();

                if (TestContext.CurrentContext.Test.Name.StartsWith("TestEventTypes"))
                    StopGrids(); // clean events for other tests
            }
        }

        /// <summary>
        /// Tests enable/disable of event types.
        /// </summary>
        [Test]
        public void TestEnableDisable()
        {
            var events = grid1.Events();

            Assert.AreEqual(0, events.GetEnabledEvents().Length);

            Assert.IsFalse(EventType.EVTS_CACHE.Any(events.IsEnabled));

            events.EnableLocal(EventType.EVTS_CACHE);

            Assert.AreEqual(EventType.EVTS_CACHE, events.GetEnabledEvents());

            Assert.IsTrue(EventType.EVTS_CACHE.All(events.IsEnabled));

            events.EnableLocal(EventType.EVTS_TASK_EXECUTION);

            events.DisableLocal(EventType.EVTS_CACHE);

            Assert.AreEqual(EventType.EVTS_TASK_EXECUTION, events.GetEnabledEvents());
        }

        /// <summary>
        /// Tests LocalListen.
        /// </summary>
        [Test]
        public void TestLocalListen()
        {
            var events = grid1.Events();
            var listener = EventsTestHelper.GetListener();
            var eventType = EventType.EVTS_TASK_EXECUTION;

            events.EnableLocal(eventType);

            events.LocalListen(listener, eventType);

            CheckSend(3);  // 3 events per task * 3 grids

            // Check unsubscription for specific event
            events.StopLocalListen(listener, EventType.EVT_TASK_REDUCED);

            CheckSend(2);

            // Unsubscribe from all events
            events.StopLocalListen(listener);

            CheckNoEvent();

            // Check unsubscription by filter
            events.LocalListen(listener, EventType.EVT_TASK_REDUCED);

            CheckSend();

            EventsTestHelper.listenResult = false;

            CheckSend();  // one last event will be received for each listener

            CheckNoEvent();
        }

        /// <summary>
        /// Tests LocalListen.
        /// </summary>
        [Test]
        [Ignore("IGNITE-879")]
        public void TestLocalListenRepeatedSubscription()
        {
            var events = grid1.Events();
            var listener = EventsTestHelper.GetListener();
            var eventType = EventType.EVTS_TASK_EXECUTION;

            events.EnableLocal(eventType);

            events.LocalListen(listener, eventType);

            CheckSend(3);  // 3 events per task * 3 grids

            events.LocalListen(listener, eventType);
            events.LocalListen(listener, eventType);

            CheckSend(9);

            events.StopLocalListen(listener, eventType);

            CheckSend(6);

            events.StopLocalListen(listener, eventType);

            CheckSend(3);
            
            events.StopLocalListen(listener, eventType);

            CheckNoEvent();
        }

        /// <summary>
        /// Tests all available event types/classes.
        /// </summary>
        [Test, TestCaseSource("TestCases")]
        public void TestEventTypes(EventTestCase testCase)
        {
            var events = grid1.Events();

            events.EnableLocal(testCase.EventType);

            var listener = EventsTestHelper.GetListener();

            events.LocalListen(listener, testCase.EventType);

            EventsTestHelper.ClearReceived(testCase.EventCount);

            testCase.GenerateEvent(grid1);

            EventsTestHelper.VerifyReceive(testCase.EventCount, testCase.EventObjectType, testCase.EventType);

            if (testCase.VerifyEvents != null)
                testCase.VerifyEvents(EventsTestHelper.RECEIVED_EVENTS.Reverse(), grid1);

            // Check stop
            events.StopLocalListen(listener);

            EventsTestHelper.ClearReceived(0);

            testCase.GenerateEvent(grid1);

            Thread.Sleep(EventsTestHelper.TIMEOUT);
        }

        /// <summary>
        /// Test cases for TestEventTypes: type id + type + event generator.
        /// </summary>
        public IEnumerable<EventTestCase> TestCases
        {
            get
            {
                yield return new EventTestCase
                {
                    EventType = EventType.EVTS_CACHE,
                    EventObjectType = typeof (CacheEvent),
                    GenerateEvent = g => g.Cache<int, int>(null).Put(1, 1),
                    VerifyEvents = (e, g) => VerifyCacheEvents(e, g),
                    EventCount = 1
                };

                yield return new EventTestCase
                {
                    EventType = EventType.EVTS_TASK_EXECUTION,
                    EventObjectType = typeof (TaskEvent),
                    GenerateEvent = g => GenerateTaskEvent(g),
                    VerifyEvents = (e, g) => VerifyTaskEvents(e),
                    EventCount = 3
                };

                yield return new EventTestCase
                {
                    EventType = EventType.EVTS_JOB_EXECUTION,
                    EventObjectType = typeof (JobEvent),
                    GenerateEvent = g => GenerateTaskEvent(g),
                    EventCount = 9
                };

                yield return new EventTestCase
                {
                    EventType = new[] {EventType.EVT_CACHE_QUERY_EXECUTED},
                    EventObjectType = typeof (CacheQueryExecutedEvent),
                    GenerateEvent = g => GenerateCacheQueryEvent(g),
                    EventCount = 1
                };

                yield return new EventTestCase
                {
                    EventType = new[] { EventType.EVT_CACHE_QUERY_OBJECT_READ },
                    EventObjectType = typeof (CacheQueryReadEvent),
                    GenerateEvent = g => GenerateCacheQueryEvent(g),
                    EventCount = 1
                };
            }
        }

        /// <summary>
        /// Tests the LocalQuery.
        /// </summary>
        [Test]
        public void TestLocalQuery()
        {
            var events = grid1.Events();

            var eventType = EventType.EVTS_TASK_EXECUTION;

            events.EnableLocal(eventType);

            var oldEvents = events.LocalQuery();

            GenerateTaskEvent();

            // "Except" works because of overridden equality
            var qryResult = events.LocalQuery(eventType).Except(oldEvents).ToList();

            Assert.AreEqual(3, qryResult.Count);
        }

        /// <summary>
        /// Tests the WaitForLocal.
        /// </summary>
        [Test]
        public void TestWaitForLocal([Values(true, false)] bool async)
        {
            var events = grid1.Events();

            var timeout = TimeSpan.FromSeconds(3);

            if (async)
                events = events.WithAsync();

            var eventType = EventType.EVTS_TASK_EXECUTION;

            events.EnableLocal(eventType);

            Func<Func<IEvent>, Task<IEvent>> getWaitTask;

            if (async)
                getWaitTask = func =>
                {
                    Assert.IsNull(func());
                    var task = events.GetFuture<IEvent>().ToTask();
                    GenerateTaskEvent();
                    return task;
                };
            else
                getWaitTask = func =>
                {
                    var task = Task.Factory.StartNew(func);
                    Thread.Sleep(500); // allow task to start and begin waiting for events
                    GenerateTaskEvent();
                    return task;
                };

            // No params
            var waitTask = getWaitTask(() => events.WaitForLocal());

            waitTask.Wait(timeout);

            // Event types
            waitTask = getWaitTask(() => events.WaitForLocal(EventType.EVT_TASK_REDUCED));

            Assert.IsTrue(waitTask.Wait(timeout));
            Assert.IsInstanceOf(typeof(TaskEvent), waitTask.Result);
            Assert.AreEqual(EventType.EVT_TASK_REDUCED, waitTask.Result.Type);

            // Filter
            waitTask = getWaitTask(() => events.WaitForLocal(
                new EventFilter<IEvent>((g, e) => e.Type == EventType.EVT_TASK_REDUCED)));

            Assert.IsTrue(waitTask.Wait(timeout));
            Assert.IsInstanceOf(typeof(TaskEvent), waitTask.Result);
            Assert.AreEqual(EventType.EVT_TASK_REDUCED, waitTask.Result.Type);

            // Filter & types
            waitTask = getWaitTask(() => events.WaitForLocal(
                new EventFilter<IEvent>((g, e) => e.Type == EventType.EVT_TASK_REDUCED), EventType.EVT_TASK_REDUCED));

            Assert.IsTrue(waitTask.Wait(timeout));
            Assert.IsInstanceOf(typeof(TaskEvent), waitTask.Result);
            Assert.AreEqual(EventType.EVT_TASK_REDUCED, waitTask.Result.Type);
        }

        /// <summary>
        /// Tests RemoteListen.
        /// </summary>
        [Test]
        public void TestRemoteListen(
            [Values(true, false)] bool async, 
            [Values(true, false)] bool portable,
            [Values(true, false)] bool autoUnsubscribe)
        {
            foreach (var g in grids)
            {
                g.Events().EnableLocal(EventType.EVTS_JOB_EXECUTION);
                g.Events().EnableLocal(EventType.EVTS_TASK_EXECUTION);
            }

            var events = grid1.Events();

            var expectedType = EventType.EVT_JOB_STARTED;

            var remoteFilter = portable 
                ?  (IEventFilter<IEvent>) new RemoteEventPortableFilter(expectedType) 
                :  new RemoteEventFilter(expectedType);

            var localListener = EventsTestHelper.GetListener();

            if (async)
                events = events.WithAsync();

            var listenId = events.RemoteListen(localListener: localListener, remoteFilter: remoteFilter,
                autoUnsubscribe: autoUnsubscribe);

            if (async)
                listenId = events.GetFuture<Guid>().Get();

            CheckSend(3, typeof(JobEvent), expectedType);

            grid3.Events().DisableLocal(EventType.EVTS_JOB_EXECUTION);

            CheckSend(2, typeof(JobEvent), expectedType);

            events.StopRemoteListen(listenId);

            if (async)
                events.GetFuture().Get();

            CheckNoEvent();

            // Check unsubscription with listener
            events.RemoteListen(localListener: localListener, remoteFilter: remoteFilter,
                autoUnsubscribe: autoUnsubscribe);

            if (async)
                events.GetFuture<Guid>().Get();

            CheckSend(2, typeof(JobEvent), expectedType);

            EventsTestHelper.listenResult = false;

            CheckSend(1, typeof(JobEvent), expectedType);  // one last event

            CheckNoEvent();
        }

        /// <summary>
        /// Tests RemoteQuery.
        /// </summary>
        [Test]
        public void TestRemoteQuery([Values(true, false)] bool async)
        {
            foreach (var g in grids)
                g.Events().EnableLocal(EventType.EVTS_JOB_EXECUTION);

            var events = grid1.Events();

            var eventFilter = new RemoteEventFilter(EventType.EVT_JOB_STARTED);

            var oldEvents = events.RemoteQuery(eventFilter);

            if (async)
                events = events.WithAsync();

            GenerateTaskEvent();

            var remoteQuery = events.RemoteQuery(eventFilter, EventsTestHelper.TIMEOUT, EventType.EVTS_JOB_EXECUTION);

            if (async)
            {
                Assert.IsNull(remoteQuery);

                remoteQuery = events.GetFuture<List<IEvent>>().Get().ToList();
            }

            var qryResult = remoteQuery.Except(oldEvents).Cast<JobEvent>().ToList();

            Assert.AreEqual(grids.Length, qryResult.Count);

            Assert.IsTrue(qryResult.All(x => x.Type == EventType.EVT_JOB_STARTED));
        }

        /// <summary>
        /// Tests serialization.
        /// </summary>
        [Test]
        public void TestSerialization()
        {
            var grid = (Ignite) grid1;
            var comp = (Impl.Compute.Compute) grid.Cluster.ForLocal().Compute();
            var locNode = grid.Cluster.LocalNode;

            var expectedGuid = Guid.Parse("00000000-0000-0001-0000-000000000002");
            var expectedGridGuid = new GridGuid(expectedGuid, 3);

            using (var inStream = GridManager.Memory.Allocate().Stream())
            {
                var result = comp.ExecuteJavaTask<bool>("org.gridgain.interop.GridInteropEventsWriteEventTask",
                    inStream.MemoryPointer);

                Assert.IsTrue(result);

                inStream.SynchronizeInput();

                var reader = grid.Marshaller.StartUnmarshal(inStream);

                var licEvent = EventReader.Read<LicenseEvent>(reader);
                Assert.AreEqual(expectedGuid, licEvent.LicenseId);
                CheckEventBase(licEvent);

                var cacheEvent = EventReader.Read<CacheEvent>(reader);
                CheckEventBase(cacheEvent);
                Assert.AreEqual("cacheName", cacheEvent.CacheName);
                Assert.AreEqual(locNode, cacheEvent.EventNode);
                Assert.AreEqual(1, cacheEvent.Partition);
                Assert.AreEqual(true, cacheEvent.IsNear);
                Assert.AreEqual(2, cacheEvent.Key);
                Assert.AreEqual(expectedGridGuid, cacheEvent.Xid);
                Assert.AreEqual(3, cacheEvent.LockId);
                Assert.AreEqual(4, cacheEvent.NewValue);
                Assert.AreEqual(true, cacheEvent.HasNewValue);
                Assert.AreEqual(5, cacheEvent.OldValue);
                Assert.AreEqual(true, cacheEvent.HasOldValue);
                Assert.AreEqual(expectedGuid, cacheEvent.SubjectId);
                Assert.AreEqual("cloClsName", cacheEvent.ClosureClassName);
                Assert.AreEqual("taskName", cacheEvent.TaskName);

                var qryExecEvent = EventReader.Read<CacheQueryExecutedEvent>(reader);
                CheckEventBase(qryExecEvent);
                Assert.AreEqual("qryType", qryExecEvent.QueryType);
                Assert.AreEqual("cacheName", qryExecEvent.CacheName);
                Assert.AreEqual("clsName", qryExecEvent.ClassName);
                Assert.AreEqual("clause", qryExecEvent.Clause);
                Assert.AreEqual(expectedGuid, qryExecEvent.SubjectId);
                Assert.AreEqual("taskName", qryExecEvent.TaskName);

                var qryReadEvent = EventReader.Read<CacheQueryReadEvent>(reader);
                CheckEventBase(qryReadEvent);
                Assert.AreEqual("qryType", qryReadEvent.QueryType);
                Assert.AreEqual("cacheName", qryReadEvent.CacheName);
                Assert.AreEqual("clsName", qryReadEvent.ClassName);
                Assert.AreEqual("clause", qryReadEvent.Clause);
                Assert.AreEqual(expectedGuid, qryReadEvent.SubjectId);
                Assert.AreEqual("taskName", qryReadEvent.TaskName);
                Assert.AreEqual(1, qryReadEvent.Key);
                Assert.AreEqual(2, qryReadEvent.Value);
                Assert.AreEqual(3, qryReadEvent.OldValue);
                Assert.AreEqual(4, qryReadEvent.Row);

                var cacheRebalancingEvent = EventReader.Read<CacheRebalancingEvent>(reader);
                CheckEventBase(cacheRebalancingEvent);
                Assert.AreEqual("cacheName", cacheRebalancingEvent.CacheName);
                Assert.AreEqual(1, cacheRebalancingEvent.Partition);
                Assert.AreEqual(locNode, cacheRebalancingEvent.DiscoveryNode);
                Assert.AreEqual(2, cacheRebalancingEvent.DiscoveryEventType);
                Assert.AreEqual(3, cacheRebalancingEvent.DiscoveryTimestamp);
                
                var checkpointEvent = EventReader.Read<CheckpointEvent>(reader);
                CheckEventBase(checkpointEvent);
                Assert.AreEqual("cpKey", checkpointEvent.Key);
                
                var discoEvent = EventReader.Read<DiscoveryEvent>(reader);
                CheckEventBase(discoEvent);
                Assert.AreEqual(grid.TopologyVersion, discoEvent.TopologyVersion);
                Assert.AreEqual(grid.Nodes(), discoEvent.TopologyNodes);

                var jobEvent = EventReader.Read<JobEvent>(reader);
                CheckEventBase(jobEvent);
                Assert.AreEqual(expectedGridGuid, jobEvent.JobId);
                Assert.AreEqual("taskClsName", jobEvent.TaskClassName);
                Assert.AreEqual("taskName", jobEvent.TaskName);
                Assert.AreEqual(locNode, jobEvent.TaskNode);
                Assert.AreEqual(expectedGridGuid, jobEvent.TaskSessionId);
                Assert.AreEqual(expectedGuid, jobEvent.TaskSubjectId);

                var spaceEvent = EventReader.Read<SwapSpaceEvent>(reader);
                CheckEventBase(spaceEvent);
                Assert.AreEqual("space", spaceEvent.Space);

                var taskEvent = EventReader.Read<TaskEvent>(reader);
                CheckEventBase(taskEvent);
                Assert.AreEqual(true,taskEvent.Internal);
                Assert.AreEqual(expectedGuid, taskEvent.SubjectId);
                Assert.AreEqual("taskClsName", taskEvent.TaskClassName);
                Assert.AreEqual("taskName", taskEvent.TaskName);
                Assert.AreEqual(expectedGridGuid, taskEvent.TaskSessionId);
            }
        }

        /// <summary>
        /// Checks base event fields serialization.
        /// </summary>
        /// <param name="evt">The evt.</param>
        private void CheckEventBase(IEvent evt)
        {
            var locNode = grid1.Cluster.LocalNode;

            Assert.AreEqual(locNode, evt.Node);
            Assert.AreEqual("msg", evt.Message);
            Assert.AreEqual(EventType.EVT_LIC_CLEARED, evt.Type);
            Assert.IsNotNullOrEmpty(evt.Name);
            Assert.AreNotEqual(Guid.Empty, evt.Id.GlobalId);
            Assert.IsTrue((evt.TimeStamp - DateTime.Now).TotalSeconds < 10);
        }

        /// <summary>
        /// Sends events in various ways and verifies correct receive.
        /// </summary>
        /// <param name="repeat">Expected event count multiplier.</param>
        /// <param name="eventObjectType">Expected event object type.</param>
        /// <param name="eventType">Type of the event.</param>
        private void CheckSend(int repeat = 1, Type eventObjectType = null, params int[] eventType)
        {
            EventsTestHelper.ClearReceived(repeat);

            GenerateTaskEvent();

            EventsTestHelper.VerifyReceive(repeat, eventObjectType ?? typeof (TaskEvent),
                eventType.Any() ? eventType : EventType.EVTS_TASK_EXECUTION);
        }

        /// <summary>
        /// Checks that no event has arrived.
        /// </summary>
        private void CheckNoEvent()
        {
            // this will result in an exception in case of a event
            EventsTestHelper.ClearReceived(0);

            GenerateTaskEvent();

            Thread.Sleep(EventsTestHelper.TIMEOUT);

            EventsTestHelper.AssertFailures();
        }

        /// <summary>
        /// Gets the grid configuration.
        /// </summary>
        private static GridConfiguration Configuration(string springConfigUrl)
        {
            return new GridConfiguration
            {
                SpringConfigUrl = springConfigUrl,
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations = new List<PortableTypeConfiguration>
                    {
                        new PortableTypeConfiguration(typeof (RemoteEventPortableFilter))
                    }
                }
            };
        }

        /// <summary>
        /// Generates the task event.
        /// </summary>
        private void GenerateTaskEvent(IIgnite grid = null)
        {
            (grid ?? grid1).Compute().Broadcast(new ComputeAction());
        }

        /// <summary>
        /// Verifies the task events.
        /// </summary>
        private static void VerifyTaskEvents(IEnumerable<IEvent> events)
        {
            var e = events.Cast<TaskEvent>().ToArray();

            // started, reduced, finished
            Assert.AreEqual(
                new[] {EventType.EVT_TASK_STARTED, EventType.EVT_TASK_REDUCED, EventType.EVT_TASK_FINISHED},
                e.Select(x => x.Type).ToArray());
        }

        /// <summary>
        /// Generates the cache query event.
        /// </summary>
        private static void GenerateCacheQueryEvent(IIgnite g)
        {
            var cache = g.Cache<int, int>(null);

            cache.Clear();

            cache.Put(1, 1);

            cache.Query(new ScanQuery<int, int>()).GetAll();
        }

        /// <summary>
        /// Verifies the cache events.
        /// </summary>
        private static void VerifyCacheEvents(IEnumerable<IEvent> events, IIgnite grid)
        {
            var e = events.Cast<CacheEvent>().ToArray();

            foreach (var cacheEvent in e)
            {
                Assert.AreEqual(null, cacheEvent.CacheName);
                Assert.AreEqual(null, cacheEvent.ClosureClassName);
                Assert.AreEqual(null, cacheEvent.TaskName);
                Assert.AreEqual(grid.Cluster.LocalNode, cacheEvent.EventNode);
                Assert.AreEqual(grid.Cluster.LocalNode, cacheEvent.Node);

                Assert.AreEqual(false, cacheEvent.HasOldValue);
                Assert.AreEqual(null, cacheEvent.OldValue);

                if (cacheEvent.Type == EventType.EVT_CACHE_OBJECT_PUT)
                {
                    Assert.AreEqual(true, cacheEvent.HasNewValue);
                    Assert.AreEqual(1, cacheEvent.NewValue);
                }
                else if (cacheEvent.Type == EventType.EVT_CACHE_ENTRY_CREATED)
                {
                    Assert.AreEqual(false, cacheEvent.HasNewValue);
                    Assert.AreEqual(null, cacheEvent.NewValue);
                }
                else
                {
                    Assert.Fail("Unexpected event type");
                }
            }
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (grid1 != null)
                return;

            grid1 = Ignition.Start(Configuration("config\\compute\\compute-grid1.xml"));
            grid2 = Ignition.Start(Configuration("config\\compute\\compute-grid2.xml"));
            grid3 = Ignition.Start(Configuration("config\\compute\\compute-grid3.xml"));

            grids = new[] {grid1, grid2, grid3};
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            grid1 = grid2 = grid3 = null;
            grids = null;
            
            Ignition.StopAll(true);
        }
    }

    /// <summary>
    /// Event test helper class.
    /// </summary>
    [Serializable]
    public static class EventsTestHelper
    {
        /** */
        public static readonly ConcurrentStack<IEvent> RECEIVED_EVENTS = new ConcurrentStack<IEvent>();
        
        /** */
        public static readonly ConcurrentStack<string> FAILURES = new ConcurrentStack<string>();

        /** */
        public static readonly CountdownEvent RECEIVED_EVENT = new CountdownEvent(0);

        /** */
        public static readonly ConcurrentStack<Guid> LAST_NODE_IDS = new ConcurrentStack<Guid>();

        /** */
        public static volatile bool listenResult = true;

        /** */
        public static readonly TimeSpan TIMEOUT = TimeSpan.FromMilliseconds(800);

        /// <summary>
        /// Clears received event information.
        /// </summary>
        /// <param name="expectedCount">The expected count of events to be received.</param>
        public static void ClearReceived(int expectedCount)
        {
            RECEIVED_EVENTS.Clear();
            RECEIVED_EVENT.Reset(expectedCount);
            LAST_NODE_IDS.Clear();
        }

        /// <summary>
        /// Verifies received events against events events.
        /// </summary>
        public static void VerifyReceive(int count, Type eventObjectType, params int[] eventTypes)
        {
            // check if expected event count has been received; Wait returns false if there were none.
            Assert.IsTrue(RECEIVED_EVENT.Wait(TIMEOUT), 
                "Failed to receive expected number of events. Remaining count: " + RECEIVED_EVENT.CurrentCount);
            
            Assert.AreEqual(count, RECEIVED_EVENTS.Count);

            Assert.IsTrue(RECEIVED_EVENTS.All(x => x.GetType() == eventObjectType));

            Assert.IsTrue(RECEIVED_EVENTS.All(x => eventTypes.Contains(x.Type)));

            AssertFailures();
        }

        /// <summary>
        /// Gets the event listener.
        /// </summary>
        /// <returns>New instance of event listener.</returns>
        public static IEventFilter<IEvent> GetListener()
        {
            return new EventFilter<IEvent>(Listen);
        }

        /// <summary>
        /// Combines accumulated failures and throws an assertion, if there are any.
        /// Clears accumulated failures.
        /// </summary>
        public static void AssertFailures()
        {
            try
            {
                if (FAILURES.Any())
                    Assert.Fail(FAILURES.Reverse().Aggregate((x, y) => string.Format("{0}\n{1}", x, y)));
            }
            finally 
            {
                FAILURES.Clear();
            }
        }

        /// <summary>
        /// Listen method.
        /// </summary>
        /// <param name="id">Originating node ID.</param>
        /// <param name="evt">Event.</param>
        private static bool Listen(Guid id, IEvent evt)
        {
            try
            {
                LAST_NODE_IDS.Push(id);
                RECEIVED_EVENTS.Push(evt);

                RECEIVED_EVENT.Signal();
                
                return listenResult;
            }
            catch (Exception ex)
            {
                // When executed on remote nodes, these exceptions will not go to sender, 
                // so we have to accumulate them.
                FAILURES.Push(string.Format("Exception in Listen (msg: {0}, id: {1}): {2}", evt, id, ex));
                throw;
            }
        }
    }

    /// <summary>
    /// Test event filter.
    /// </summary>
    [Serializable]
    public class EventFilter<T> : IEventFilter<T> where T : IEvent
    {
        /** */
        private readonly Func<Guid, T, bool> invoke;

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteListenEventFilter"/> class.
        /// </summary>
        /// <param name="invoke">The invoke delegate.</param>
        public EventFilter(Func<Guid, T, bool> invoke)
        {
            this.invoke = invoke;
        }

        /** <inheritdoc /> */
        bool IEventFilter<T>.Invoke(Guid nodeId, T evt)
        {
            return invoke(nodeId, evt);
        }

        /** <inheritdoc /> */
        public bool Invoke(Guid nodeId, T evt)
        {
            throw new Exception("Invalid method");
        }
    }

    /// <summary>
    /// Remote event filter.
    /// </summary>
    [Serializable]
    public class RemoteEventFilter : IEventFilter<IEvent>
    {
        /** */
        private readonly int type;

        public RemoteEventFilter(int type)
        {
            this.type = type;
        }

        /** <inheritdoc /> */
        public bool Invoke(Guid nodeId, IEvent evt)
        {
            return evt.Type == type;
        }
    }

    /// <summary>
    /// Portable remote event filter.
    /// </summary>
    public class RemoteEventPortableFilter : IEventFilter<IEvent>, IPortableMarshalAware
    {
        /** */
        private int type;

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteEventPortableFilter"/> class.
        /// </summary>
        /// <param name="type">The event type.</param>
        public RemoteEventPortableFilter(int type)
        {
            this.type = type;
        }

        /** <inheritdoc /> */
        public bool Invoke(Guid nodeId, IEvent evt)
        {
            return evt.Type == type;
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            writer.RawWriter().WriteInt(type);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            type = reader.RawReader().ReadInt();
        }
    }

    /// <summary>
    /// Event test case.
    /// </summary>
    public class EventTestCase
    {
        /// <summary>
        /// Gets or sets the type of the event.
        /// </summary>
        public int[] EventType { get; set; }

        /// <summary>
        /// Gets or sets the type of the event object.
        /// </summary>
        public Type EventObjectType { get; set; }

        /// <summary>
        /// Gets or sets the generate event action.
        /// </summary>
        public Action<IIgnite> GenerateEvent { get; set; }

        /// <summary>
        /// Gets or sets the verify events action.
        /// </summary>
        public Action<IEnumerable<IEvent>, IIgnite> VerifyEvents { get; set; }

        /// <summary>
        /// Gets or sets the event count.
        /// </summary>
        public int EventCount { get; set; }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return EventObjectType.ToString();
        }
    }
}
