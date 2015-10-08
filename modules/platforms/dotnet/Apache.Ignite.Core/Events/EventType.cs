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

namespace Apache.Ignite.Core.Events
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection;

    using Apache.Ignite.Core.Impl.Collections;

    /// <summary>
    /// Contains event type constants. The decision to use class and not enumeration is dictated 
    /// by allowing users to create their own events and/or event types which would be impossible with enumerations.
    /// <para />
    /// Note that this interface defines not only individual type constants, 
    /// but arrays of types as well to be conveniently used with <see cref="IEvents"/> methods.
    /// <para />
    /// NOTE: all types in range <b>from 1 to 1000 are reserved</b> for internal Ignite events 
    /// and should not be used by user-defined events.
    /// </summary>
    public static class EventType
    {
        /// <summary>
        /// Built-in event type: checkpoint was saved.
        /// </summary>
        public static readonly int EventCheckpointSaved = 1;

        /// <summary>
        /// Built-in event type: checkpoint was loaded.
        /// </summary>
        public static readonly int EventCheckpointLoaded = 2;

        /// <summary>
        /// Built-in event type: checkpoint was removed. Reasons are: timeout expired, or or it was manually removed, 
        /// or it was automatically removed by the task session.
        /// </summary>
        public static readonly int EventCheckpointRemoved = 3;

        /// <summary>
        /// Built-in event type: node joined topology. New node has been discovered and joined grid topology. Note that 
        /// even though a node has been discovered there could be a number of warnings in the log. In certain 
        /// situations Ignite doesn't prevent a node from joining but prints warning messages into the log.
        /// </summary>
        public static readonly int EventNodeJoined = 10;

        /// <summary>
        /// Built-in event type: node has normally left topology.
        /// </summary>
        public static readonly int EventNodeLeft = 11;

        /// <summary>
        /// Built-in event type: node failed. Ignite detected that node has presumably crashed and is considered 
        /// failed.
        /// </summary>
        public static readonly int EventNodeFailed = 12;

        /// <summary>
        /// Built-in event type: node metrics updated. Generated when node's metrics are updated. In most cases this 
        /// callback is invoked with every heartbeat received from a node (including local node).
        /// </summary>
        public static readonly int EventNodeMetricsUpdated = 13;

        /// <summary>
        /// Built-in event type: local node segmented. Generated when node determines that it runs in invalid network 
        /// segment.
        /// </summary>
        public static readonly int EventNodeSegmented = 14;

        /// <summary>
        /// Built-in event type: client node disconnected.
        /// </summary>
        public static readonly int EventClientNodeDisconnected = 16;

        /// <summary>
        /// Built-in event type: client node reconnected.
        /// </summary>
        public static readonly int EventClientNodeReconnected = 17;

        /// <summary>
        /// Built-in event type: task started.
        /// </summary>
        public static readonly int EventTaskStarted = 20;

        /// <summary>
        /// Built-in event type: task finished. Task got finished. This event is triggered every time a task finished 
        /// without exception.
        /// </summary>
        public static readonly int EventTaskFinished = 21;

        /// <summary>
        /// Built-in event type: task failed. Task failed. This event is triggered every time a task finished with an 
        /// exception. Note that prior to this event, there could be other events recorded specific to the failure.
        /// </summary>
        public static readonly int EventTaskFailed = 22;

        /// <summary>
        /// Built-in event type: task timed out.
        /// </summary>
        public static readonly int EventTaskTimedout = 23;

        /// <summary>
        /// Built-in event type: task session attribute set.
        /// </summary>
        public static readonly int EventTaskSessionAttrSet = 24;

        /// <summary>
        /// Built-in event type: task reduced.
        /// </summary>
        public static readonly int EventTaskReduced = 25;

        /// <summary>
        /// Built-in event type: Ignite job was mapped in {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} 
        /// method.
        /// </summary>
        public static readonly int EventJobMapped = 40;

        /// <summary>
        /// Built-in event type: Ignite job result was received by {@link 
        /// org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} method.
        /// </summary>
        public static readonly int EventJobResulted = 41;

        /// <summary>
        /// Built-in event type: Ignite job failed over.
        /// </summary>
        public static readonly int EventJobFailedOver = 43;

        /// <summary>
        /// Built-in event type: Ignite job started.
        /// </summary>
        public static readonly int EventJobStarted = 44;

        /// <summary>
        /// Built-in event type: Ignite job finished. Job has successfully completed and produced a result which from the 
        /// user perspective can still be either negative or positive.
        /// </summary>
        public static readonly int EventJobFinished = 45;

        /// <summary>
        /// Built-in event type: Ignite job timed out.
        /// </summary>
        public static readonly int EventJobTimedout = 46;

        /// <summary>
        /// Built-in event type: Ignite job rejected during collision resolution.
        /// </summary>
        public static readonly int EventJobRejected = 47;

        /// <summary>
        /// Built-in event type: Ignite job failed. Job has failed. This means that there was some error event during job 
        /// execution and job did not produce a result.
        /// </summary>
        public static readonly int EventJobFailed = 48;
        
        /// <summary>
        /// Built-in event type: Ignite job queued. Job arrived for execution and has been queued (added to passive queue 
        /// during collision resolution).
        /// </summary>
        public static readonly int EventJobQueued = 49;

        /// <summary>
        /// Built-in event type: Ignite job cancelled.
        /// </summary>
        public static readonly int EventJobCancelled = 50;

        /// <summary>
        /// Built-in event type: entry created.
        /// </summary>
        public static readonly int EventCacheEntryCreated = 60;

        /// <summary>
        /// Built-in event type: entry destroyed.
        /// </summary>
        public static readonly int EventCacheEntryDestroyed = 61;

        /// <summary>
        /// Built-in event type: entry evicted.
        /// </summary>
        public static readonly int EventCacheEntryEvicted = 62;

        /// <summary>
        /// Built-in event type: object put.
        /// </summary>
        public static readonly int EventCacheObjectPut = 63;

        /// <summary>
        /// Built-in event type: object read.
        /// </summary>
        public static readonly int EventCacheObjectRead = 64;

        /// <summary>
        /// Built-in event type: object removed.
        /// </summary>
        public static readonly int EventCacheObjectRemoved = 65;

        /// <summary>
        /// Built-in event type: object locked.
        /// </summary>
        public static readonly int EventCacheObjectLocked = 66;

        /// <summary>
        /// Built-in event type: object unlocked.
        /// </summary>
        public static readonly int EventCacheObjectUnlocked = 67;

        /// <summary>
        /// Built-in event type: cache object swapped from swap storage.
        /// </summary>
        public static readonly int EventCacheObjectSwapped = 68;

        /// <summary>
        /// Built-in event type: cache object unswapped from swap storage.
        /// </summary>
        public static readonly int EventCacheObjectUnswapped = 69;

        /// <summary>
        /// Built-in event type: cache object was expired when reading it.
        /// </summary>
        public static readonly int EventCacheObjectExpired = 70;

        /// <summary>
        /// Built-in event type: swap space data read.
        /// </summary>
        public static readonly int EventSwapSpaceDataRead = 71;

        /// <summary>
        /// Built-in event type: swap space data stored.
        /// </summary>
        public static readonly int EventSwapSpaceDataStored = 72;

        /// <summary>
        /// Built-in event type: swap space data removed.
        /// </summary>
        public static readonly int EventSwapSpaceDataRemoved = 73;

        /// <summary>
        /// Built-in event type: swap space cleared.
        /// </summary>
        public static readonly int EventSwapSpaceCleared = 74;

        /// <summary>
        /// Built-in event type: swap space data evicted.
        /// </summary>
        public static readonly int EventSwapSpaceDataEvicted = 75;

        /// <summary>
        /// Built-in event type: cache object stored in off-heap storage.
        /// </summary>
        public static readonly int EventCacheObjectToOffheap = 76;

        /// <summary>
        /// Built-in event type: cache object moved from off-heap storage back into memory.
        /// </summary>
        public static readonly int EventCacheObjectFromOffheap = 77;

        /// <summary>
        /// Built-in event type: cache rebalance started.
        /// </summary>
        public static readonly int EventCacheRebalanceStarted = 80;

        /// <summary>
        /// Built-in event type: cache rebalance stopped.
        /// </summary>
        public static readonly int EventCacheRebalanceStopped = 81;

        /// <summary>
        /// Built-in event type: cache partition loaded.
        /// </summary>
        public static readonly int EventCacheRebalancePartLoaded = 82;

        /// <summary>
        /// Built-in event type: cache partition unloaded.
        /// </summary>
        public static readonly int EventCacheRebalancePartUnloaded = 83;

        /// <summary>
        /// Built-in event type: cache entry rebalanced.
        /// </summary>
        public static readonly int EventCacheRebalanceObjectLoaded = 84;

        /// <summary>
        /// Built-in event type: cache entry unloaded.
        /// </summary>
        public static readonly int EventCacheRebalanceObjectUnloaded = 85;

        /// <summary>
        /// Built-in event type: all nodes that hold partition left topology.
        /// </summary>
        public static readonly int EventCacheRebalancePartDataLost = 86;

        /// <summary>
        /// Built-in event type: query executed.
        /// </summary>
        public static readonly int EventCacheQueryExecuted = 96;

        /// <summary>
        /// Built-in event type: query entry read.
        /// </summary>
        public static readonly int EventCacheQueryObjectRead = 97;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int EventCacheStarted = 98;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int EventCacheStopped = 99;

        /// <summary>
        /// Built-in event type: cache nodes left.
        /// </summary>
        public static readonly int EventCacheNodesLeft = 100;

        /// <summary>
        /// All events indicating an error or failure condition. It is convenient to use when fetching all events 
        /// indicating error or failure.
        /// </summary>
        private static readonly ICollection<int> EventsError0 = new[]
        {
            EventJobTimedout,
            EventJobFailed,
            EventJobFailedOver,
            EventJobRejected,
            EventJobCancelled,
            EventTaskTimedout,
            EventTaskFailed,
            EventCacheRebalanceStarted,
            EventCacheRebalanceStopped
        }.AsReadOnly();

        /// <summary>
        /// All discovery events except for <see cref="EventNodeMetricsUpdated" />. Subscription to <see 
        /// cref="EventNodeMetricsUpdated" /> can generate massive amount of event processing in most cases is not 
        /// necessary. If this event is indeed required you can subscribe to it individually or use <see 
        /// cref="EventsDiscoveryAll0" /> array.
        /// </summary>
        private static readonly ICollection<int> EventsDiscovery0 = new[]
        {
            EventNodeJoined,
            EventNodeLeft,
            EventNodeFailed,
            EventNodeSegmented,
            EventClientNodeDisconnected,
            EventClientNodeReconnected
        }.AsReadOnly();

        /// <summary>
        /// All discovery events.
        /// </summary>
        private static readonly ICollection<int> EventsDiscoveryAll0 = new[]
        {
            EventNodeJoined,
            EventNodeLeft,
            EventNodeFailed,
            EventNodeSegmented,
            EventNodeMetricsUpdated,
            EventClientNodeDisconnected,
            EventClientNodeReconnected
        }.AsReadOnly();

        /// <summary>
        /// All Ignite job execution events.
        /// </summary>
        private static readonly ICollection<int> EventsJobExecution0 = new[]
        {
            EventJobMapped,
            EventJobResulted,
            EventJobFailedOver,
            EventJobStarted,
            EventJobFinished,
            EventJobTimedout,
            EventJobRejected,
            EventJobFailed,
            EventJobQueued,
            EventJobCancelled
        }.AsReadOnly();

        /// <summary>
        /// All Ignite task execution events.
        /// </summary>
        private static readonly ICollection<int> EventsTaskExecution0 = new[]
        {
            EventTaskStarted,
            EventTaskFinished,
            EventTaskFailed,
            EventTaskTimedout,
            EventTaskSessionAttrSet,
            EventTaskReduced
        }.AsReadOnly();

        /// <summary>
        /// All cache events.
        /// </summary>
        private static readonly ICollection<int> EventsCache0 = new[]
        {
            EventCacheEntryCreated,
            EventCacheEntryDestroyed,
            EventCacheObjectPut,
            EventCacheObjectRead,
            EventCacheObjectRemoved,
            EventCacheObjectLocked,
            EventCacheObjectUnlocked,
            EventCacheObjectSwapped,
            EventCacheObjectUnswapped,
            EventCacheObjectExpired
        }.AsReadOnly();

        /// <summary>
        /// All cache rebalance events.
        /// </summary>
        private static readonly ICollection<int> EventsCacheRebalance0 = new[]
        {
            EventCacheRebalanceStarted,
            EventCacheRebalanceStopped,
            EventCacheRebalancePartLoaded,
            EventCacheRebalancePartUnloaded,
            EventCacheRebalanceObjectLoaded,
            EventCacheRebalanceObjectUnloaded,
            EventCacheRebalancePartDataLost
        }.AsReadOnly();

        /// <summary>
        /// All cache lifecycle events.
        /// </summary>
        private static readonly ICollection<int> EventsCacheLifecycle0 = new[]
        {
            EventCacheStarted,
            EventCacheStopped,
            EventCacheNodesLeft
        }.AsReadOnly();

        /// <summary>
        /// All cache query events.
        /// </summary>
        private static readonly ICollection<int> EventsCacheQuery0 = new[]
        {
            EventCacheQueryExecuted,
            EventCacheQueryObjectRead
        }.AsReadOnly();

        /// <summary>
        /// All swap space events.
        /// </summary>
        private static readonly ICollection<int> EventsSwapspace0 = new[]
        {
            EventSwapSpaceCleared,
            EventSwapSpaceDataRemoved,
            EventSwapSpaceDataRead,
            EventSwapSpaceDataStored,
            EventSwapSpaceDataEvicted
        }.AsReadOnly();

        /// <summary>
        /// All Ignite events.
        /// </summary>
        private static readonly ICollection<int> EventsAll0 = GetAllEvents().AsReadOnly();

        /// <summary>
        /// All Ignite events (<b>excluding</b> metric update event).
        /// </summary>
        private static readonly ICollection<int> EventsAllMinusMetricUpdate0 =
            EventsAll0.Where(x => x != EventNodeMetricsUpdated).ToArray().AsReadOnly();

        /// <summary>
        /// All events indicating an error or failure condition. It is convenient to use when fetching all events 
        /// indicating error or failure.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsError
        {
            get { return EventsError0; }
        }

        /// <summary>
        /// All Ignite events (<b>excluding</b> metric update event).
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsAllMinusMetricUpdate
        {
            get { return EventsAllMinusMetricUpdate0; }
        }

        /// <summary>
        /// All swap space events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsSwapspace
        {
            get { return EventsSwapspace0; }
        }

        /// <summary>
        /// All cache query events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsCacheQuery
        {
            get { return EventsCacheQuery0; }
        }

        /// <summary>
        /// All cache lifecycle events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsCacheLifecycle
        {
            get { return EventsCacheLifecycle0; }
        }

        /// <summary>
        /// All cache rebalance events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsCacheRebalance
        {
            get { return EventsCacheRebalance0; }
        }

        /// <summary>
        /// All cache events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsCache
        {
            get { return EventsCache0; }
        }

        /// <summary>
        /// All Ignite task execution events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsTaskExecution
        {
            get { return EventsTaskExecution0; }
        }

        /// <summary>
        /// All Ignite job execution events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsJobExecution
        {
            get { return EventsJobExecution0; }
        }

        /// <summary>
        /// All discovery events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsDiscoveryAll
        {
            get { return EventsDiscoveryAll0; }
        }

        /// <summary>
        /// All discovery events except for <see cref="EventNodeMetricsUpdated" />. Subscription to <see 
        /// cref="EventNodeMetricsUpdated" /> can generate massive amount of event processing in most cases is not 
        /// necessary. If this event is indeed required you can subscribe to it individually or use <see 
        /// cref="EventsDiscoveryAll0" /> array.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsDiscovery
        {
            get { return EventsDiscovery0; }
        }

        /// <summary>
        /// All Ignite events.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays")]
        public static ICollection<int> EventsAll
        {
            get { return EventsAll0; }
        }

        /// <summary>
        /// Gets all the events.
        /// </summary>
        /// <returns>All event ids.</returns>
        private static int[] GetAllEvents()
        {
            return typeof (EventType).GetFields(BindingFlags.Public | BindingFlags.Static)
                .Where(x => x.FieldType == typeof (int))
                .Select(x => (int) x.GetValue(null)).ToArray();
        }
    }
}