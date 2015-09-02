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
        public static readonly int EVT_CHECKPOINT_SAVED = 1;

        /// <summary>
        /// Built-in event type: checkpoint was loaded.
        /// </summary>
        public static readonly int EVT_CHECKPOINT_LOADED = 2;

        /// <summary>
        /// Built-in event type: checkpoint was removed. Reasons are: timeout expired, or or it was manually removed, 
        /// or it was automatically removed by the task session.
        /// </summary>
        public static readonly int EVT_CHECKPOINT_REMOVED = 3;

        /// <summary>
        /// Built-in event type: node joined topology. New node has been discovered and joined grid topology. Note that 
        /// even though a node has been discovered there could be a number of warnings in the log. In certain 
        /// situations Ignite doesn't prevent a node from joining but prints warning messages into the log.
        /// </summary>
        public static readonly int EVT_NODE_JOINED = 10;

        /// <summary>
        /// Built-in event type: node has normally left topology.
        /// </summary>
        public static readonly int EVT_NODE_LEFT = 11;

        /// <summary>
        /// Built-in event type: node failed. Ignite detected that node has presumably crashed and is considered 
        /// failed.
        /// </summary>
        public static readonly int EVT_NODE_FAILED = 12;

        /// <summary>
        /// Built-in event type: node metrics updated. Generated when node's metrics are updated. In most cases this 
        /// callback is invoked with every heartbeat received from a node (including local node).
        /// </summary>
        public static readonly int EVT_NODE_METRICS_UPDATED = 13;

        /// <summary>
        /// Built-in event type: local node segmented. Generated when node determines that it runs in invalid network 
        /// segment.
        /// </summary>
        public static readonly int EVT_NODE_SEGMENTED = 14;

        /// <summary>
        /// Built-in event type: client node disconnected.
        /// </summary>
        public static readonly int EVT_CLIENT_NODE_DISCONNECTED = 16;

        /// <summary>
        /// Built-in event type: client node reconnected.
        /// </summary>
        public static readonly int EVT_CLIENT_NODE_RECONNECTED = 17;

        /// <summary>
        /// Built-in event type: task started.
        /// </summary>
        public static readonly int EVT_TASK_STARTED = 20;

        /// <summary>
        /// Built-in event type: task finished. Task got finished. This event is triggered every time a task finished 
        /// without exception.
        /// </summary>
        public static readonly int EVT_TASK_FINISHED = 21;

        /// <summary>
        /// Built-in event type: task failed. Task failed. This event is triggered every time a task finished with an 
        /// exception. Note that prior to this event, there could be other events recorded specific to the failure.
        /// </summary>
        public static readonly int EVT_TASK_FAILED = 22;

        /// <summary>
        /// Built-in event type: task timed out.
        /// </summary>
        public static readonly int EVT_TASK_TIMEDOUT = 23;

        /// <summary>
        /// Built-in event type: task session attribute set.
        /// </summary>
        public static readonly int EVT_TASK_SESSION_ATTR_SET = 24;

        /// <summary>
        /// Built-in event type: task reduced.
        /// </summary>
        public static readonly int EVT_TASK_REDUCED = 25;

        /// <summary>
        /// Built-in event type: grid job was mapped in {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} 
        /// method.
        /// </summary>
        public static readonly int EVT_JOB_MAPPED = 40;

        /// <summary>
        /// Built-in event type: grid job result was received by {@link 
        /// org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} method.
        /// </summary>
        public static readonly int EVT_JOB_RESULTED = 41;

        /// <summary>
        /// Built-in event type: grid job failed over.
        /// </summary>
        public static readonly int EVT_JOB_FAILED_OVER = 43;

        /// <summary>
        /// Built-in event type: grid job started.
        /// </summary>
        public static readonly int EVT_JOB_STARTED = 44;

        /// <summary>
        /// Built-in event type: grid job finished. Job has successfully completed and produced a result which from the 
        /// user perspective can still be either negative or positive.
        /// </summary>
        public static readonly int EVT_JOB_FINISHED = 45;

        /// <summary>
        /// Built-in event type: grid job timed out.
        /// </summary>
        public static readonly int EVT_JOB_TIMEDOUT = 46;

        /// <summary>
        /// Built-in event type: grid job rejected during collision resolution.
        /// </summary>
        public static readonly int EVT_JOB_REJECTED = 47;

        /// <summary>
        /// Built-in event type: grid job failed. Job has failed. This means that there was some error event during job 
        /// execution and job did not produce a result.
        /// </summary>
        public static readonly int EVT_JOB_FAILED = 48;

        /// <summary>
        /// Built-in event type: grid job queued. Job arrived for execution and has been queued (added to passive queue 
        /// during collision resolution).
        /// </summary>
        public static readonly int EVT_JOB_QUEUED = 49;

        /// <summary>
        /// Built-in event type: grid job cancelled.
        /// </summary>
        public static readonly int EVT_JOB_CANCELLED = 50;

        /// <summary>
        /// Built-in event type: entry created.
        /// </summary>
        public static readonly int EVT_CACHE_ENTRY_CREATED = 60;

        /// <summary>
        /// Built-in event type: entry destroyed.
        /// </summary>
        public static readonly int EVT_CACHE_ENTRY_DESTROYED = 61;

        /// <summary>
        /// Built-in event type: entry evicted.
        /// </summary>
        public static readonly int EVT_CACHE_ENTRY_EVICTED = 62;

        /// <summary>
        /// Built-in event type: object put.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_PUT = 63;

        /// <summary>
        /// Built-in event type: object read.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_READ = 64;

        /// <summary>
        /// Built-in event type: object removed.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_REMOVED = 65;

        /// <summary>
        /// Built-in event type: object locked.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_LOCKED = 66;

        /// <summary>
        /// Built-in event type: object unlocked.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_UNLOCKED = 67;

        /// <summary>
        /// Built-in event type: cache object swapped from swap storage.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_SWAPPED = 68;

        /// <summary>
        /// Built-in event type: cache object unswapped from swap storage.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_UNSWAPPED = 69;

        /// <summary>
        /// Built-in event type: cache object was expired when reading it.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_EXPIRED = 70;

        /// <summary>
        /// Built-in event type: swap space data read.
        /// </summary>
        public static readonly int EVT_SWAP_SPACE_DATA_READ = 71;

        /// <summary>
        /// Built-in event type: swap space data stored.
        /// </summary>
        public static readonly int EVT_SWAP_SPACE_DATA_STORED = 72;

        /// <summary>
        /// Built-in event type: swap space data removed.
        /// </summary>
        public static readonly int EVT_SWAP_SPACE_DATA_REMOVED = 73;

        /// <summary>
        /// Built-in event type: swap space cleared.
        /// </summary>
        public static readonly int EVT_SWAP_SPACE_CLEARED = 74;

        /// <summary>
        /// Built-in event type: swap space data evicted.
        /// </summary>
        public static readonly int EVT_SWAP_SPACE_DATA_EVICTED = 75;

        /// <summary>
        /// Built-in event type: cache object stored in off-heap storage.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_TO_OFFHEAP = 76;

        /// <summary>
        /// Built-in event type: cache object moved from off-heap storage back into memory.
        /// </summary>
        public static readonly int EVT_CACHE_OBJECT_FROM_OFFHEAP = 77;

        /// <summary>
        /// Built-in event type: cache rebalance started.
        /// </summary>
        public static readonly int EVT_CACHE_REBALANCE_STARTED = 80;

        /// <summary>
        /// Built-in event type: cache rebalance stopped.
        /// </summary>
        public static readonly int EVT_CACHE_REBALANCE_STOPPED = 81;

        /// <summary>
        /// Built-in event type: cache partition loaded.
        /// </summary>
        public static readonly int EVT_CACHE_REBALANCE_PART_LOADED = 82;

        /// <summary>
        /// Built-in event type: cache partition unloaded.
        /// </summary>
        public static readonly int EVT_CACHE_REBALANCE_PART_UNLOADED = 83;

        /// <summary>
        /// Built-in event type: cache entry rebalanced.
        /// </summary>
        public static readonly int EVT_CACHE_REBALANCE_OBJECT_LOADED = 84;

        /// <summary>
        /// Built-in event type: cache entry unloaded.
        /// </summary>
        public static readonly int EVT_CACHE_REBALANCE_OBJECT_UNLOADED = 85;

        /// <summary>
        /// Built-in event type: all nodes that hold partition left topology.
        /// </summary>
        public static readonly int EVT_CACHE_REBALANCE_PART_DATA_LOST = 86;

        /// <summary>
        /// Built-in event type: query executed.
        /// </summary>
        public static readonly int EVT_CACHE_QUERY_EXECUTED = 96;

        /// <summary>
        /// Built-in event type: query entry read.
        /// </summary>
        public static readonly int EVT_CACHE_QUERY_OBJECT_READ = 97;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int EVT_CACHE_STARTED = 98;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int EVT_CACHE_STOPPED = 99;

        /// <summary>
        /// Built-in event type: cache nodes left.
        /// </summary>
        public static readonly int EVT_CACHE_NODES_LEFT = 100;

        /// <summary>
        /// Built-in event type: license violation detected.
        /// </summary>
        public static readonly int EVT_LIC_VIOLATION = 1008;

        /// <summary>
        /// Built-in event type: license violation cleared.
        /// </summary>
        public static readonly int EVT_LIC_CLEARED = 1009;

        /// <summary>
        /// Built-in event type: license violation grace period is expired.
        /// </summary>
        public static readonly int EVT_LIC_GRACE_EXPIRED = 1010;

        /// <summary>
        /// Built-in event type: license violation detected.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_LICENSE =
        {
            EVT_LIC_CLEARED,
            EVT_LIC_VIOLATION,
            EVT_LIC_GRACE_EXPIRED
        };

        /// <summary>
        /// All checkpoint events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_CHECKPOINT =
        {
            EVT_CHECKPOINT_SAVED,
            EVT_CHECKPOINT_LOADED,
            EVT_CHECKPOINT_REMOVED
        };

        /// <summary>
        /// All events indicating an error or failure condition. It is convenient to use when fetching all events 
        /// indicating error or failure.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_ERROR =
        {
            EVT_JOB_TIMEDOUT,
            EVT_JOB_FAILED,
            EVT_JOB_FAILED_OVER,
            EVT_JOB_REJECTED,
            EVT_JOB_CANCELLED,
            EVT_TASK_TIMEDOUT,
            EVT_TASK_FAILED,
            EVT_CACHE_REBALANCE_STARTED,
            EVT_CACHE_REBALANCE_STOPPED
        };

        /// <summary>
        /// All discovery events except for <see cref="EVT_NODE_METRICS_UPDATED" />. Subscription to <see 
        /// cref="EVT_NODE_METRICS_UPDATED" /> can generate massive amount of event processing in most cases is not 
        /// necessary. If this event is indeed required you can subscribe to it individually or use <see 
        /// cref="EVTS_DISCOVERY_ALL" /> array.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_DISCOVERY =
        {
            EVT_NODE_JOINED,
            EVT_NODE_LEFT,
            EVT_NODE_FAILED,
            EVT_NODE_SEGMENTED,
            EVT_CLIENT_NODE_DISCONNECTED,
            EVT_CLIENT_NODE_RECONNECTED
        };

        /// <summary>
        /// All discovery events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_DISCOVERY_ALL =
        {
            EVT_NODE_JOINED,
            EVT_NODE_LEFT,
            EVT_NODE_FAILED,
            EVT_NODE_SEGMENTED,
            EVT_NODE_METRICS_UPDATED,
            EVT_CLIENT_NODE_DISCONNECTED,
            EVT_CLIENT_NODE_RECONNECTED
        };

        /// <summary>
        /// All grid job execution events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_JOB_EXECUTION =
        {
            EVT_JOB_MAPPED,
            EVT_JOB_RESULTED,
            EVT_JOB_FAILED_OVER,
            EVT_JOB_STARTED,
            EVT_JOB_FINISHED,
            EVT_JOB_TIMEDOUT,
            EVT_JOB_REJECTED,
            EVT_JOB_FAILED,
            EVT_JOB_QUEUED,
            EVT_JOB_CANCELLED
        };

        /// <summary>
        /// All grid task execution events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_TASK_EXECUTION =
        {
            EVT_TASK_STARTED,
            EVT_TASK_FINISHED,
            EVT_TASK_FAILED,
            EVT_TASK_TIMEDOUT,
            EVT_TASK_SESSION_ATTR_SET,
            EVT_TASK_REDUCED
        };

        /// <summary>
        /// All cache events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_CACHE =
        {
            EVT_CACHE_ENTRY_CREATED,
            EVT_CACHE_ENTRY_DESTROYED,
            EVT_CACHE_OBJECT_PUT,
            EVT_CACHE_OBJECT_READ,
            EVT_CACHE_OBJECT_REMOVED,
            EVT_CACHE_OBJECT_LOCKED,
            EVT_CACHE_OBJECT_UNLOCKED,
            EVT_CACHE_OBJECT_SWAPPED,
            EVT_CACHE_OBJECT_UNSWAPPED,
            EVT_CACHE_OBJECT_EXPIRED
        };

        /// <summary>
        /// All cache rebalance events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_CACHE_REBALANCE =
        {
            EVT_CACHE_REBALANCE_STARTED,
            EVT_CACHE_REBALANCE_STOPPED,
            EVT_CACHE_REBALANCE_PART_LOADED,
            EVT_CACHE_REBALANCE_PART_UNLOADED,
            EVT_CACHE_REBALANCE_OBJECT_LOADED,
            EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
            EVT_CACHE_REBALANCE_PART_DATA_LOST
        };

        /// <summary>
        /// All cache lifecycle events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_CACHE_LIFECYCLE =
        {
            EVT_CACHE_STARTED,
            EVT_CACHE_STOPPED,
            EVT_CACHE_NODES_LEFT
        };

        /// <summary>
        /// All cache query events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_CACHE_QUERY =
        {
            EVT_CACHE_QUERY_EXECUTED,
            EVT_CACHE_QUERY_OBJECT_READ
        };

        /// <summary>
        /// All swap space events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_SWAPSPACE =
        {
            EVT_SWAP_SPACE_CLEARED,
            EVT_SWAP_SPACE_DATA_REMOVED,
            EVT_SWAP_SPACE_DATA_READ,
            EVT_SWAP_SPACE_DATA_STORED,
            EVT_SWAP_SPACE_DATA_EVICTED
        };

        /// <summary>
        /// All grid events.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_ALL = GetAllEvents();

        /// <summary>
        /// All grid events (<b>excluding</b> metric update event).
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2105:ArrayFieldsShouldNotBeReadOnly",
            Justification = "Breaking change. Should be fixed in the next non-compatible release.")]
        public static readonly int[] EVTS_ALL_MINUS_METRIC_UPDATE =
            EVTS_ALL.Where(x => x != EVT_NODE_METRICS_UPDATED).ToArray();

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