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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Events;
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// <see cref="IEvents"/> tests.
    /// </summary>
    public class EventsTest
    {
        /** */
        private IIgnite _grid1;

        /** */
        private IIgnite _grid2;

        /** */
        private IIgnite _grid3;

        /** */
        private IIgnite[] _grids;
        
        /** */
        public static int IdGen;

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
            EventsTestHelper.ListenResult = true;
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public virtual void TearDown()
        {
            try
            {
                TestUtils.AssertHandleRegistryIsEmpty(1000, _grid1, _grid2, _grid3);
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
            var events = _grid1.GetEvents();

            Assert.AreEqual(0, events.GetEnabledEvents().Count);
            
            Assert.IsFalse(EventType.CacheAll.Any(events.IsEnabled));

            events.EnableLocal(EventType.CacheAll);

            Assert.AreEqual(EventType.CacheAll, events.GetEnabledEvents());

            Assert.IsTrue(EventType.CacheAll.All(events.IsEnabled));

            events.EnableLocal(EventType.TaskExecutionAll);

            events.DisableLocal(EventType.CacheAll);

            Assert.AreEqual(EventType.TaskExecutionAll, events.GetEnabledEvents());
        }

        /// <summary>
        /// Tests LocalListen.
        /// </summary>
        [Test]
        public void TestLocalListen()
        {
            var events = _grid1.GetEvents();
            var listener = EventsTestHelper.GetListener();
            var eventType = EventType.TaskExecutionAll;

            events.EnableLocal(eventType);

            events.LocalListen(listener, eventType);

            CheckSend(3);  // 3 events per task * 3 grids

            // Check unsubscription for specific event
            events.StopLocalListen(listener, EventType.TaskReduced);

            CheckSend(2);

            // Unsubscribe from all events
            events.StopLocalListen(listener);

            CheckNoEvent();

            // Check unsubscription by filter
            events.LocalListen(listener, EventType.TaskReduced);

            CheckSend();

            EventsTestHelper.ListenResult = false;

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
            var events = _grid1.GetEvents();
            var listener = EventsTestHelper.GetListener();
            var eventType = EventType.TaskExecutionAll;

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
            var events = _grid1.GetEvents();

            events.EnableLocal(testCase.EventType);

            var listener = EventsTestHelper.GetListener();

            events.LocalListen(listener, testCase.EventType);

            EventsTestHelper.ClearReceived(testCase.EventCount);

            testCase.GenerateEvent(_grid1);

            EventsTestHelper.VerifyReceive(testCase.EventCount, testCase.EventObjectType, testCase.EventType);

            if (testCase.VerifyEvents != null)
                testCase.VerifyEvents(EventsTestHelper.ReceivedEvents.Reverse(), _grid1);

            // Check stop
            events.StopLocalListen(listener);

            EventsTestHelper.ClearReceived(0);

            testCase.GenerateEvent(_grid1);

            Thread.Sleep(EventsTestHelper.Timeout);
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
                    EventType = EventType.CacheAll,
                    EventObjectType = typeof (CacheEvent),
                    GenerateEvent = g => g.GetCache<int, int>(null).Put(1, 1),
                    VerifyEvents = (e, g) => VerifyCacheEvents(e, g),
                    EventCount = 2
                };

                yield return new EventTestCase
                {
                    EventType = EventType.TaskExecutionAll,
                    EventObjectType = typeof (TaskEvent),
                    GenerateEvent = g => GenerateTaskEvent(g),
                    VerifyEvents = (e, g) => VerifyTaskEvents(e),
                    EventCount = 3
                };

                yield return new EventTestCase
                {
                    EventType = EventType.JobExecutionAll,
                    EventObjectType = typeof (JobEvent),
                    GenerateEvent = g => GenerateTaskEvent(g),
                    EventCount = 7
                };
                
                yield return new EventTestCase
                {
                    EventType = new[] {EventType.CacheQueryExecuted},
                    EventObjectType = typeof (CacheQueryExecutedEvent),
                    GenerateEvent = g => GenerateCacheQueryEvent(g),
                    EventCount = 1
                };

                yield return new EventTestCase
                {
                    EventType = new[] { EventType.CacheQueryObjectRead },
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
            var events = _grid1.GetEvents();

            var eventType = EventType.TaskExecutionAll;

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
            var events = _grid1.GetEvents();

            var timeout = TimeSpan.FromSeconds(3);

            var eventType = EventType.TaskExecutionAll;

            events.EnableLocal(eventType);

            Func<IEventFilter<IEvent>, int[], Task<IEvent>> getWaitTask;

            if (async)
                getWaitTask = (filter, types) =>
                {
                    var task = events.WaitForLocalAsync(filter, types);
                    GenerateTaskEvent();
                    return task;
                };
            else
                getWaitTask = (filter, types) =>
                {
                    var task = Task.Factory.StartNew(() => events.WaitForLocal(filter, types));
                    Thread.Sleep(500); // allow task to start and begin waiting for events
                    GenerateTaskEvent();
                    return task;
                };

            // No params
            var waitTask = getWaitTask(null, new int[0]);

            waitTask.Wait(timeout);

            // Event types
            waitTask = getWaitTask(null, new[] {EventType.TaskReduced});

            Assert.IsTrue(waitTask.Wait(timeout));
            Assert.IsInstanceOf(typeof(TaskEvent), waitTask.Result);
            Assert.AreEqual(EventType.TaskReduced, waitTask.Result.Type);

            // Filter
            waitTask = getWaitTask(new EventFilter<IEvent>(e => e.Type == EventType.TaskReduced), new int[0]);

            Assert.IsTrue(waitTask.Wait(timeout));
            Assert.IsInstanceOf(typeof(TaskEvent), waitTask.Result);
            Assert.AreEqual(EventType.TaskReduced, waitTask.Result.Type);

            // Filter & types
            waitTask = getWaitTask(new EventFilter<IEvent>(e => e.Type == EventType.TaskReduced),
                new[] {EventType.TaskReduced});

            Assert.IsTrue(waitTask.Wait(timeout));
            Assert.IsInstanceOf(typeof(TaskEvent), waitTask.Result);
            Assert.AreEqual(EventType.TaskReduced, waitTask.Result.Type);
        }

        /*
        /// <summary>
        /// Tests RemoteListen.
        /// </summary>
        [Test]
        public void TestRemoteListen(
            [Values(true, false)] bool async, 
            [Values(true, false)] bool binarizable,
            [Values(true, false)] bool autoUnsubscribe)
        {
            foreach (var g in _grids)
            {
                g.GetEvents().EnableLocal(EventType.JobExecutionAll);
                g.GetEvents().EnableLocal(EventType.TaskExecutionAll);
            }

            var events = _grid1.GetEvents();

            var expectedType = EventType.JobStarted;

            var remoteFilter = binary 
                ?  (IEventFilter<IEvent>) new RemoteEventBinarizableFilter(expectedType)
                :  new RemoteEventFilter(expectedType);

            var localListener = EventsTestHelper.GetListener();

            if (async)
                events = events.WithAsync();

            var listenId = events.RemoteListen(localListener: localListener, remoteFilter: remoteFilter,
                autoUnsubscribe: autoUnsubscribe);

            if (async)
                listenId = events.GetFuture<Guid>().Get();

            Assert.IsNotNull(listenId);

            CheckSend(3, typeof(JobEvent), expectedType);

            _grid3.GetEvents().DisableLocal(EventType.JobExecutionAll);

            CheckSend(2, typeof(JobEvent), expectedType);

            events.StopRemoteListen(listenId.Value);

            if (async)
                events.GetFuture().Get();

            CheckNoEvent();

            // Check unsubscription with listener
            events.RemoteListen(localListener: localListener, remoteFilter: remoteFilter,
                autoUnsubscribe: autoUnsubscribe);

            if (async)
                events.GetFuture<Guid>().Get();

            CheckSend(2, typeof(JobEvent), expectedType);

            EventsTestHelper.ListenResult = false;

            CheckSend(1, typeof(JobEvent), expectedType);  // one last event

            CheckNoEvent();
        }*/

        /// <summary>
        /// Tests RemoteQuery.
        /// </summary>
        [Test]
        public void TestRemoteQuery([Values(true, false)] bool async)
        {
            foreach (var g in _grids)
                g.GetEvents().EnableLocal(EventType.JobExecutionAll);

            var events = _grid1.GetEvents();

            var eventFilter = new RemoteEventFilter(EventType.JobStarted);

            var oldEvents = events.RemoteQuery(eventFilter);

            GenerateTaskEvent();

            var remoteQuery = !async 
                ? events.RemoteQuery(eventFilter, EventsTestHelper.Timeout, EventType.JobExecutionAll) 
                : events.RemoteQueryAsync(eventFilter, EventsTestHelper.Timeout, EventType.JobExecutionAll).Result;

            var qryResult = remoteQuery.Except(oldEvents).Cast<JobEvent>().ToList();

            Assert.AreEqual(_grids.Length - 1, qryResult.Count);

            Assert.IsTrue(qryResult.All(x => x.Type == EventType.JobStarted));
        }

        /// <summary>
        /// Tests serialization.
        /// </summary>
        [Test]
        public void TestSerialization()
        {
            var grid = (Ignite) _grid1;
            var comp = (Impl.Compute.Compute) grid.GetCluster().ForLocal().GetCompute();
            var locNode = grid.GetCluster().GetLocalNode();

            var expectedGuid = Guid.Parse("00000000-0000-0001-0000-000000000002");
            var expectedGridGuid = new IgniteGuid(expectedGuid, 3);

            using (var inStream = IgniteManager.Memory.Allocate().GetStream())
            {
                var result = comp.ExecuteJavaTask<bool>("org.apache.ignite.platform.PlatformEventsWriteEventTask",
                    inStream.MemoryPointer);

                Assert.IsTrue(result);

                inStream.SynchronizeInput();

                var reader = grid.Marshaller.StartUnmarshal(inStream);

                var cacheEvent = EventReader.Read<CacheEvent>(reader);
                CheckEventBase(cacheEvent);
                Assert.AreEqual("cacheName", cacheEvent.CacheName);
                Assert.AreEqual(locNode, cacheEvent.EventNode);
                Assert.AreEqual(1, cacheEvent.Partition);
                Assert.AreEqual(true, cacheEvent.IsNear);
                Assert.AreEqual(2, cacheEvent.Key);
                Assert.AreEqual(expectedGridGuid, cacheEvent.Xid);
                Assert.AreEqual(null, cacheEvent.LockId);
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
                Assert.AreEqual(grid.GetNodes(), discoEvent.TopologyNodes);

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
            var locNode = _grid1.GetCluster().GetLocalNode();

            Assert.AreEqual(locNode, evt.Node);
            Assert.AreEqual("msg", evt.Message);
            Assert.AreEqual(EventType.SwapSpaceCleared, evt.Type);
            Assert.IsNotNullOrEmpty(evt.Name);
            Assert.AreNotEqual(Guid.Empty, evt.Id.GlobalId);
            Assert.IsTrue(Math.Abs((evt.Timestamp - DateTime.UtcNow).TotalSeconds) < 20, 
                "Invalid event timestamp: '{0}', current time: '{1}'", evt.Timestamp, DateTime.Now);
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
                eventType.Any() ? eventType : EventType.TaskExecutionAll);
        }

        /// <summary>
        /// Checks that no event has arrived.
        /// </summary>
        private void CheckNoEvent()
        {
            // this will result in an exception in case of a event
            EventsTestHelper.ClearReceived(0);

            GenerateTaskEvent();

            Thread.Sleep(EventsTestHelper.Timeout);

            EventsTestHelper.AssertFailures();
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private static IgniteConfiguration Configuration(string springConfigUrl)
        {
            return new IgniteConfiguration
            {
                SpringConfigUrl = springConfigUrl,
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof (RemoteEventBinarizableFilter))
                    }
                }
            };
        }

        /// <summary>
        /// Generates the task event.
        /// </summary>
        private void GenerateTaskEvent(IIgnite grid = null)
        {
            (grid ?? _grid1).GetCompute().Broadcast(new ComputeAction());
        }

        /// <summary>
        /// Verifies the task events.
        /// </summary>
        private static void VerifyTaskEvents(IEnumerable<IEvent> events)
        {
            var e = events.Cast<TaskEvent>().ToArray();

            // started, reduced, finished
            Assert.AreEqual(
                new[] {EventType.TaskStarted, EventType.TaskReduced, EventType.TaskFinished},
                e.Select(x => x.Type).ToArray());
        }

        /// <summary>
        /// Generates the cache query event.
        /// </summary>
        private static void GenerateCacheQueryEvent(IIgnite g)
        {
            var cache = g.GetCache<int, int>(null);

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
                Assert.AreEqual(grid.GetCluster().GetLocalNode(), cacheEvent.EventNode);
                Assert.AreEqual(grid.GetCluster().GetLocalNode(), cacheEvent.Node);

                Assert.AreEqual(false, cacheEvent.HasOldValue);
                Assert.AreEqual(null, cacheEvent.OldValue);

                if (cacheEvent.Type == EventType.CacheObjectPut)
                {
                    Assert.AreEqual(true, cacheEvent.HasNewValue);
                    Assert.AreEqual(1, cacheEvent.NewValue);
                }
                else if (cacheEvent.Type == EventType.CacheEntryCreated)
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
            if (_grid1 != null)
                return;

            _grid1 = Ignition.Start(Configuration("config\\compute\\compute-grid1.xml"));
            _grid2 = Ignition.Start(Configuration("config\\compute\\compute-grid2.xml"));
            _grid3 = Ignition.Start(Configuration("config\\compute\\compute-grid3.xml"));

            _grids = new[] {_grid1, _grid2, _grid3};
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            _grid1 = _grid2 = _grid3 = null;
            _grids = null;
            
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
        public static readonly ConcurrentStack<IEvent> ReceivedEvents = new ConcurrentStack<IEvent>();
        
        /** */
        public static readonly ConcurrentStack<string> Failures = new ConcurrentStack<string>();

        /** */
        public static readonly CountdownEvent ReceivedEvent = new CountdownEvent(0);

        /** */
        public static readonly ConcurrentStack<Guid?> LastNodeIds = new ConcurrentStack<Guid?>();

        /** */
        public static volatile bool ListenResult = true;

        /** */
        public static readonly TimeSpan Timeout = TimeSpan.FromMilliseconds(800);

        /// <summary>
        /// Clears received event information.
        /// </summary>
        /// <param name="expectedCount">The expected count of events to be received.</param>
        public static void ClearReceived(int expectedCount)
        {
            ReceivedEvents.Clear();
            ReceivedEvent.Reset(expectedCount);
            LastNodeIds.Clear();
        }

        /// <summary>
        /// Verifies received events against events events.
        /// </summary>
        public static void VerifyReceive(int count, Type eventObjectType, ICollection<int> eventTypes)
        {
            // check if expected event count has been received; Wait returns false if there were none.
            Assert.IsTrue(ReceivedEvent.Wait(Timeout), 
                "Failed to receive expected number of events. Remaining count: " + ReceivedEvent.CurrentCount);
            
            Assert.AreEqual(count, ReceivedEvents.Count);

            Assert.IsTrue(ReceivedEvents.All(x => x.GetType() == eventObjectType));

            Assert.IsTrue(ReceivedEvents.All(x => eventTypes.Contains(x.Type)));

            AssertFailures();
        }

        /// <summary>
        /// Gets the event listener.
        /// </summary>
        /// <returns>New instance of event listener.</returns>
        public static IEventListener<IEvent> GetListener()
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
                if (Failures.Any())
                    Assert.Fail(Failures.Reverse().Aggregate((x, y) => string.Format("{0}\n{1}", x, y)));
            }
            finally 
            {
                Failures.Clear();
            }
        }

        /// <summary>
        /// Listen method.
        /// </summary>
        /// <param name="evt">Event.</param>
        private static bool Listen(IEvent evt)
        {
            try
            {
                LastNodeIds.Push(evt.Node.Id);
                ReceivedEvents.Push(evt);

                ReceivedEvent.Signal();
                
                return ListenResult;
            }
            catch (Exception ex)
            {
                // When executed on remote nodes, these exceptions will not go to sender, 
                // so we have to accumulate them.
                Failures.Push(string.Format("Exception in Listen (msg: {0}, id: {1}): {2}", evt, evt.Node.Id, ex));
                throw;
            }
        }
    }

    /// <summary>
    /// Test event filter.
    /// </summary>
    [Serializable]
    public class EventFilter<T> : IEventFilter<T>, IEventListener<T> where T : IEvent
    {
        /** */
        private readonly Func<T, bool> _invoke;

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteListenEventFilter"/> class.
        /// </summary>
        /// <param name="invoke">The invoke delegate.</param>
        public EventFilter(Func<T, bool> invoke)
        {
            _invoke = invoke;
        }

        /** <inheritdoc /> */
        bool IEventFilter<T>.Invoke(T evt)
        {
            return _invoke(evt);
        }

        /** <inheritdoc /> */
        bool IEventListener<T>.Invoke(T evt)
        {
            return _invoke(evt);
        }

        /** <inheritdoc /> */
        public bool Invoke(T evt)
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
        private readonly int _type;

        public RemoteEventFilter(int type)
        {
            _type = type;
        }

        /** <inheritdoc /> */
        public bool Invoke(IEvent evt)
        {
            return evt.Type == _type;
        }
    }

    /// <summary>
    /// Binary remote event filter.
    /// </summary>
    public class RemoteEventBinarizableFilter : IEventFilter<IEvent>, IBinarizable
    {
        /** */
        private int _type;

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteEventBinarizableFilter"/> class.
        /// </summary>
        /// <param name="type">The event type.</param>
        public RemoteEventBinarizableFilter(int type)
        {
            _type = type;
        }

        /** <inheritdoc /> */
        public bool Invoke(IEvent evt)
        {
            return evt.Type == _type;
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            writer.GetRawWriter().WriteInt(_type);
        }

        /** <inheritdoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            _type = reader.GetRawReader().ReadInt();
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
        public ICollection<int> EventType { get; set; }

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
