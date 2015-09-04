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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Reflection;

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
        public static readonly int EvtCheckpointSaved = 1;

        /// <summary>
        /// Built-in event type: checkpoint was loaded.
        /// </summary>
        public static readonly int EvtCheckpointLoaded = 2;

        /// <summary>
        /// Built-in event type: checkpoint was removed. Reasons are: timeout expired, or or it was manually removed, 
        /// or it was automatically removed by the task session.
        /// </summary>
        public static readonly int EvtCheckpointRemoved = 3;

        /// <summary>
        /// Built-in event type: node joined topology. New node has been discovered and joined grid topology. Note that 
        /// even though a node has been discovered there could be a number of warnings in the log. In certain 
        /// situations Ignite doesn't prevent a node from joining but prints warning messages into the log.
        /// </summary>
        public static readonly int EvtNodeJoined = 10;

        /// <summary>
        /// Built-in event type: node has normally left topology.
        /// </summary>
        public static readonly int EvtNodeLeft = 11;

        /// <summary>
        /// Built-in event type: node failed. Ignite detected that node has presumably crashed and is considered 
        /// failed.
        /// </summary>
        public static readonly int EvtNodeFailed = 12;

        /// <summary>
        /// Built-in event type: node metrics updated. Generated when node's metrics are updated. In most cases this 
        /// callback is invoked with every heartbeat received from a node (including local node).
        /// </summary>
        public static readonly int EvtNodeMetricsUpdated = 13;

        /// <summary>
        /// Built-in event type: local node segmented. Generated when node determines that it runs in invalid network 
        /// segment.
        /// </summary>
        public static readonly int EvtNodeSegmented = 14;

        /// <summary>
        /// Built-in event type: client node disconnected.
        /// </summary>
        public static readonly int EvtClientNodeDisconnected = 16;

        /// <summary>
        /// Built-in event type: client node reconnected.
        /// </summary>
        public static readonly int EvtClientNodeReconnected = 17;

        /// <summary>
        /// Built-in event type: task started.
        /// </summary>
        public static readonly int EvtTaskStarted = 20;

        /// <summary>
        /// Built-in event type: task finished. Task got finished. This event is triggered every time a task finished 
        /// without exception.
        /// </summary>
        public static readonly int EvtTaskFinished = 21;

        /// <summary>
        /// Built-in event type: task failed. Task failed. This event is triggered every time a task finished with an 
        /// exception. Note that prior to this event, there could be other events recorded specific to the failure.
        /// </summary>
        public static readonly int EvtTaskFailed = 22;

        /// <summary>
        /// Built-in event type: task timed out.
        /// </summary>
        public static readonly int EvtTaskTimedout = 23;

        /// <summary>
        /// Built-in event type: task session attribute set.
        /// </summary>
        public static readonly int EvtTaskSessionAttrSet = 24;

        /// <summary>
        /// Built-in event type: task reduced.
        /// </summary>
        public static readonly int EvtTaskReduced = 25;

        /// <summary>
        /// Built-in event type: Ignite job was mapped in {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} 
        /// method.
        /// </summary>
        public static readonly int EvtJobMapped = 40;

        /// <summary>
        /// Built-in event type: Ignite job result was received by {@link 
        /// org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} method.
        /// </summary>
        public static readonly int EvtJobResulted = 41;

        /// <summary>
        /// Built-in event type: Ignite job failed over.
        /// </summary>
        public static readonly int EvtJobFailedOver = 43;

        /// <summary>
        /// Built-in event type: Ignite job started.
        /// </summary>
        public static readonly int EvtJobStarted = 44;

        /// <summary>
        /// Built-in event type: Ignite job finished. Job has successfully completed and produced a result which from the 
        /// user perspective can still be either negative or positive.
        /// </summary>
        public static readonly int EvtJobFinished = 45;

        /// <summary>
        /// Built-in event type: Ignite job timed out.
        /// </summary>
        public static readonly int EvtJobTimedout = 46;

        /// <summary>
        /// Built-in event type: Ignite job rejected during collision resolution.
        /// </summary>
        public static readonly int EvtJobRejected = 47;

        /// <summary>
        /// Built-in event type: Ignite job failed. Job has failed. This means that there was some error event during job 
        /// execution and job did not produce a result.
        /// </summary>
        public static readonly int EvtJobFailed = 48;

        /// <summary>
        /// Built-in event type: Ignite job queued. Job arrived for execution and has been queued (added to passive queue 
        /// during collision resolution).
        /// </summary>
        public static readonly int EvtJobQueued = 49;

        /// <summary>
        /// Built-in event type: Ignite job cancelled.
        /// </summary>
        public static readonly int EvtJobCancelled = 50;

        /// <summary>
        /// Built-in event type: entry created.
        /// </summary>
        public static readonly int EvtCacheEntryCreated = 60;

        /// <summary>
        /// Built-in event type: entry destroyed.
        /// </summary>
        public static readonly int EvtCacheEntryDestroyed = 61;

        /// <summary>
        /// Built-in event type: entry evicted.
        /// </summary>
        public static readonly int EvtCacheEntryEvicted = 62;

        /// <summary>
        /// Built-in event type: object put.
        /// </summary>
        public static readonly int EvtCacheObjectPut = 63;

        /// <summary>
        /// Built-in event type: object read.
        /// </summary>
        public static readonly int EvtCacheObjectRead = 64;

        /// <summary>
        /// Built-in event type: object removed.
        /// </summary>
        public static readonly int EvtCacheObjectRemoved = 65;

        /// <summary>
        /// Built-in event type: object locked.
        /// </summary>
        public static readonly int EvtCacheObjectLocked = 66;

        /// <summary>
        /// Built-in event type: object unlocked.
        /// </summary>
        public static readonly int EvtCacheObjectUnlocked = 67;

        /// <summary>
        /// Built-in event type: cache object swapped from swap storage.
        /// </summary>
        public static readonly int EvtCacheObjectSwapped = 68;

        /// <summary>
        /// Built-in event type: cache object unswapped from swap storage.
        /// </summary>
        public static readonly int EvtCacheObjectUnswapped = 69;

        /// <summary>
        /// Built-in event type: cache object was expired when reading it.
        /// </summary>
        public static readonly int EvtCacheObjectExpired = 70;

        /// <summary>
        /// Built-in event type: swap space data read.
        /// </summary>
        public static readonly int EvtSwapSpaceDataRead = 71;

        /// <summary>
        /// Built-in event type: swap space data stored.
        /// </summary>
        public static readonly int EvtSwapSpaceDataStored = 72;

        /// <summary>
        /// Built-in event type: swap space data removed.
        /// </summary>
        public static readonly int EvtSwapSpaceDataRemoved = 73;

        /// <summary>
        /// Built-in event type: swap space cleared.
        /// </summary>
        public static readonly int EvtSwapSpaceCleared = 74;

        /// <summary>
        /// Built-in event type: swap space data evicted.
        /// </summary>
        public static readonly int EvtSwapSpaceDataEvicted = 75;

        /// <summary>
        /// Built-in event type: cache object stored in off-heap storage.
        /// </summary>
        public static readonly int EvtCacheObjectToOffheap = 76;

        /// <summary>
        /// Built-in event type: cache object moved from off-heap storage back into memory.
        /// </summary>
        public static readonly int EvtCacheObjectFromOffheap = 77;

        /// <summary>
        /// Built-in event type: cache rebalance started.
        /// </summary>
        public static readonly int EvtCacheRebalanceStarted = 80;

        /// <summary>
        /// Built-in event type: cache rebalance stopped.
        /// </summary>
        public static readonly int EvtCacheRebalanceStopped = 81;

        /// <summary>
        /// Built-in event type: cache partition loaded.
        /// </summary>
        public static readonly int EvtCacheRebalancePartLoaded = 82;

        /// <summary>
        /// Built-in event type: cache partition unloaded.
        /// </summary>
        public static readonly int EvtCacheRebalancePartUnloaded = 83;

        /// <summary>
        /// Built-in event type: cache entry rebalanced.
        /// </summary>
        public static readonly int EvtCacheRebalanceObjectLoaded = 84;

        /// <summary>
        /// Built-in event type: cache entry unloaded.
        /// </summary>
        public static readonly int EvtCacheRebalanceObjectUnloaded = 85;

        /// <summary>
        /// Built-in event type: all nodes that hold partition left topology.
        /// </summary>
        public static readonly int EvtCacheRebalancePartDataLost = 86;

        /// <summary>
        /// Built-in event type: query executed.
        /// </summary>
        public static readonly int EvtCacheQueryExecuted = 96;

        /// <summary>
        /// Built-in event type: query entry read.
        /// </summary>
        public static readonly int EvtCacheQueryObjectRead = 97;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int EvtCacheStarted = 98;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int EvtCacheStopped = 99;

        /// <summary>
        /// Built-in event type: cache nodes left.
        /// </summary>
        public static readonly int EvtCacheNodesLeft = 100;

        /// <summary>
        /// All events indicating an error or failure condition. It is convenient to use when fetching all events 
        /// indicating error or failure.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsError =
        {
            EvtJobTimedout,
            EvtJobFailed,
            EvtJobFailedOver,
            EvtJobRejected,
            EvtJobCancelled,
            EvtTaskTimedout,
            EvtTaskFailed,
            EvtCacheRebalanceStarted,
            EvtCacheRebalanceStopped
        };

        /// <summary>
        /// All discovery events except for <see cref="EvtNodeMetricsUpdated" />. Subscription to <see 
        /// cref="EvtNodeMetricsUpdated" /> can generate massive amount of event processing in most cases is not 
        /// necessary. If this event is indeed required you can subscribe to it individually or use <see 
        /// cref="EvtsDiscoveryAll" /> array.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsDiscovery =
        {
            EvtNodeJoined,
            EvtNodeLeft,
            EvtNodeFailed,
            EvtNodeSegmented,
            EvtClientNodeDisconnected,
            EvtClientNodeReconnected
        };

        /// <summary>
        /// All discovery events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsDiscoveryAll =
        {
            EvtNodeJoined,
            EvtNodeLeft,
            EvtNodeFailed,
            EvtNodeSegmented,
            EvtNodeMetricsUpdated,
            EvtClientNodeDisconnected,
            EvtClientNodeReconnected
        };

        /// <summary>
        /// All Ignite job execution events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsJobExecution =
        {
            EvtJobMapped,
            EvtJobResulted,
            EvtJobFailedOver,
            EvtJobStarted,
            EvtJobFinished,
            EvtJobTimedout,
            EvtJobRejected,
            EvtJobFailed,
            EvtJobQueued,
            EvtJobCancelled
        };

        /// <summary>
        /// All Ignite task execution events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsTaskExecution =
        {
            EvtTaskStarted,
            EvtTaskFinished,
            EvtTaskFailed,
            EvtTaskTimedout,
            EvtTaskSessionAttrSet,
            EvtTaskReduced
        };

        /// <summary>
        /// All cache events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsCache =
        {
            EvtCacheEntryCreated,
            EvtCacheEntryDestroyed,
            EvtCacheObjectPut,
            EvtCacheObjectRead,
            EvtCacheObjectRemoved,
            EvtCacheObjectLocked,
            EvtCacheObjectUnlocked,
            EvtCacheObjectSwapped,
            EvtCacheObjectUnswapped,
            EvtCacheObjectExpired
        };

        /// <summary>
        /// All cache rebalance events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsCacheRebalance =
        {
            EvtCacheRebalanceStarted,
            EvtCacheRebalanceStopped,
            EvtCacheRebalancePartLoaded,
            EvtCacheRebalancePartUnloaded,
            EvtCacheRebalanceObjectLoaded,
            EvtCacheRebalanceObjectUnloaded,
            EvtCacheRebalancePartDataLost
        };

        /// <summary>
        /// All cache lifecycle events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsCacheLifecycle =
        {
            EvtCacheStarted,
            EvtCacheStopped,
            EvtCacheNodesLeft
        };

        /// <summary>
        /// All cache query events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsCacheQuery =
        {
            EvtCacheQueryExecuted,
            EvtCacheQueryObjectRead
        };

        /// <summary>
        /// All swap space events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsSwapspace =
        {
            EvtSwapSpaceCleared,
            EvtSwapSpaceDataRemoved,
            EvtSwapSpaceDataRead,
            EvtSwapSpaceDataStored,
            EvtSwapSpaceDataEvicted
        };

        /// <summary>
        /// All Ignite events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsAll = GetAllEvents();

        /// <summary>
        /// All Ignite events (<b>excluding</b> metric update event).
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EvtsAllMinusMetricUpdate =
            EvtsAll.Where(x => x != EvtNodeMetricsUpdated).ToArray();

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