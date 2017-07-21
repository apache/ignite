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

// ReSharper disable ConvertToConstant.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Global
namespace Apache.Ignite.Core.Events
{
    using System.Collections.Generic;
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
        public static readonly int CheckpointSaved = 1;

        /// <summary>
        /// Built-in event type: checkpoint was loaded.
        /// </summary>
        public static readonly int CheckpointLoaded = 2;

        /// <summary>
        /// Built-in event type: checkpoint was removed. Reasons are: timeout expired, or or it was manually removed, 
        /// or it was automatically removed by the task session.
        /// </summary>
        public static readonly int CheckpointRemoved = 3;

        /// <summary>
        /// Built-in event type: node joined topology. New node has been discovered and joined grid topology. Note that 
        /// even though a node has been discovered there could be a number of warnings in the log. In certain 
        /// situations Ignite doesn't prevent a node from joining but prints warning messages into the log.
        /// </summary>
        public static readonly int NodeJoined = 10;

        /// <summary>
        /// Built-in event type: node has normally left topology.
        /// </summary>
        public static readonly int NodeLeft = 11;

        /// <summary>
        /// Built-in event type: node failed. Ignite detected that node has presumably crashed and is considered 
        /// failed.
        /// </summary>
        public static readonly int NodeFailed = 12;

        /// <summary>
        /// Built-in event type: node metrics updated. Generated when node's metrics are updated. In most cases this 
        /// callback is invoked with every heartbeat received from a node (including local node).
        /// </summary>
        public static readonly int NodeMetricsUpdated = 13;

        /// <summary>
        /// Built-in event type: local node segmented. Generated when node determines that it runs in invalid network 
        /// segment.
        /// </summary>
        public static readonly int NodeSegmented = 14;

        /// <summary>
        /// Built-in event type: client node disconnected.
        /// </summary>
        public static readonly int ClientNodeDisconnected = 16;

        /// <summary>
        /// Built-in event type: client node reconnected.
        /// </summary>
        public static readonly int ClientNodeReconnected = 17;

        /// <summary>
        /// Built-in event type: task started.
        /// </summary>
        public static readonly int TaskStarted = 20;

        /// <summary>
        /// Built-in event type: task finished. Task got finished. This event is triggered every time a task finished 
        /// without exception.
        /// </summary>
        public static readonly int TaskFinished = 21;

        /// <summary>
        /// Built-in event type: task failed. Task failed. This event is triggered every time a task finished with an 
        /// exception. Note that prior to this event, there could be other events recorded specific to the failure.
        /// </summary>
        public static readonly int TaskFailed = 22;

        /// <summary>
        /// Built-in event type: task timed out.
        /// </summary>
        public static readonly int TaskTimedout = 23;

        /// <summary>
        /// Built-in event type: task session attribute set.
        /// </summary>
        public static readonly int TaskSessionAttrSet = 24;

        /// <summary>
        /// Built-in event type: task reduced.
        /// </summary>
        public static readonly int TaskReduced = 25;

        /// <summary>
        /// Built-in event type: Ignite job was mapped in {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} 
        /// method.
        /// </summary>
        public static readonly int JobMapped = 40;

        /// <summary>
        /// Built-in event type: Ignite job result was received by {@link 
        /// org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} method.
        /// </summary>
        public static readonly int JobResulted = 41;

        /// <summary>
        /// Built-in event type: Ignite job failed over.
        /// </summary>
        public static readonly int JobFailedOver = 43;

        /// <summary>
        /// Built-in event type: Ignite job started.
        /// </summary>
        public static readonly int JobStarted = 44;

        /// <summary>
        /// Built-in event type: Ignite job finished. Job has successfully completed and produced a result which from the 
        /// user perspective can still be either negative or positive.
        /// </summary>
        public static readonly int JobFinished = 45;

        /// <summary>
        /// Built-in event type: Ignite job timed out.
        /// </summary>
        public static readonly int JobTimedout = 46;

        /// <summary>
        /// Built-in event type: Ignite job rejected during collision resolution.
        /// </summary>
        public static readonly int JobRejected = 47;

        /// <summary>
        /// Built-in event type: Ignite job failed. Job has failed. This means that there was some error event during job 
        /// execution and job did not produce a result.
        /// </summary>
        public static readonly int JobFailed = 48;
        
        /// <summary>
        /// Built-in event type: Ignite job queued. Job arrived for execution and has been queued (added to passive queue 
        /// during collision resolution).
        /// </summary>
        public static readonly int JobQueued = 49;

        /// <summary>
        /// Built-in event type: Ignite job cancelled.
        /// </summary>
        public static readonly int JobCancelled = 50;

        /// <summary>
        /// Built-in event type: entry created.
        /// </summary>
        public static readonly int CacheEntryCreated = 60;

        /// <summary>
        /// Built-in event type: entry destroyed.
        /// </summary>
        public static readonly int CacheEntryDestroyed = 61;

        /// <summary>
        /// Built-in event type: entry evicted.
        /// </summary>
        public static readonly int CacheEntryEvicted = 62;

        /// <summary>
        /// Built-in event type: object put.
        /// </summary>
        public static readonly int CacheObjectPut = 63;

        /// <summary>
        /// Built-in event type: object read.
        /// </summary>
        public static readonly int CacheObjectRead = 64;

        /// <summary>
        /// Built-in event type: object removed.
        /// </summary>
        public static readonly int CacheObjectRemoved = 65;

        /// <summary>
        /// Built-in event type: object locked.
        /// </summary>
        public static readonly int CacheObjectLocked = 66;

        /// <summary>
        /// Built-in event type: object unlocked.
        /// </summary>
        public static readonly int CacheObjectUnlocked = 67;

        /// <summary>
        /// Built-in event type: cache object was expired when reading it.
        /// </summary>
        public static readonly int CacheObjectExpired = 70;

        /// <summary>
        /// Built-in event type: cache object stored in off-heap storage.
        /// </summary>
        public static readonly int CacheObjectToOffheap = 76;

        /// <summary>
        /// Built-in event type: cache object moved from off-heap storage back into memory.
        /// </summary>
        public static readonly int CacheObjectFromOffheap = 77;

        /// <summary>
        /// Built-in event type: cache rebalance started.
        /// </summary>
        public static readonly int CacheRebalanceStarted = 80;

        /// <summary>
        /// Built-in event type: cache rebalance stopped.
        /// </summary>
        public static readonly int CacheRebalanceStopped = 81;

        /// <summary>
        /// Built-in event type: cache partition loaded.
        /// </summary>
        public static readonly int CacheRebalancePartLoaded = 82;

        /// <summary>
        /// Built-in event type: cache partition unloaded.
        /// </summary>
        public static readonly int CacheRebalancePartUnloaded = 83;

        /// <summary>
        /// Built-in event type: cache entry rebalanced.
        /// </summary>
        public static readonly int CacheRebalanceObjectLoaded = 84;

        /// <summary>
        /// Built-in event type: cache entry unloaded.
        /// </summary>
        public static readonly int CacheRebalanceObjectUnloaded = 85;

        /// <summary>
        /// Built-in event type: all nodes that hold partition left topology.
        /// </summary>
        public static readonly int CacheRebalancePartDataLost = 86;

        /// <summary>
        /// Built-in event type: query executed.
        /// </summary>
        public static readonly int CacheQueryExecuted = 96;

        /// <summary>
        /// Built-in event type: query entry read.
        /// </summary>
        public static readonly int CacheQueryObjectRead = 97;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int CacheStarted = 98;

        /// <summary>
        /// Built-in event type: cache started.
        /// </summary>
        public static readonly int CacheStopped = 99;

        /// <summary>
        /// Built-in event type: cache nodes left.
        /// </summary>
        public static readonly int CacheNodesLeft = 100;

        /// <summary>
        /// All events indicating an error or failure condition. It is convenient to use when fetching all events 
        /// indicating error or failure.
        /// </summary>
        private static readonly ICollection<int> ErrorAll0 = new[]
        {
            JobTimedout,
            JobFailed,
            JobFailedOver,
            JobRejected,
            JobCancelled,
            TaskTimedout,
            TaskFailed,
            CacheRebalanceStarted,
            CacheRebalanceStopped
        }.AsReadOnly();

        /// <summary>
        /// All discovery events except for <see cref="NodeMetricsUpdated" />. Subscription to <see 
        /// cref="NodeMetricsUpdated" /> can generate massive amount of event processing in most cases is not 
        /// necessary. If this event is indeed required you can subscribe to it individually or use <see 
        /// cref="DiscoveryAll0" /> array.
        /// </summary>
        private static readonly ICollection<int> DiscoveryAllMinusMetrics0 = new[]
        {
            NodeJoined,
            NodeLeft,
            NodeFailed,
            NodeSegmented,
            ClientNodeDisconnected,
            ClientNodeReconnected
        }.AsReadOnly();

        /// <summary>
        /// All discovery events.
        /// </summary>
        private static readonly ICollection<int> DiscoveryAll0 = new[]
        {
            NodeJoined,
            NodeLeft,
            NodeFailed,
            NodeSegmented,
            NodeMetricsUpdated,
            ClientNodeDisconnected,
            ClientNodeReconnected
        }.AsReadOnly();

        /// <summary>
        /// All Ignite job execution events.
        /// </summary>
        private static readonly ICollection<int> JobExecutionAll0 = new[]
        {
            JobMapped,
            JobResulted,
            JobFailedOver,
            JobStarted,
            JobFinished,
            JobTimedout,
            JobRejected,
            JobFailed,
            JobQueued,
            JobCancelled
        }.AsReadOnly();

        /// <summary>
        /// All Ignite task execution events.
        /// </summary>
        private static readonly ICollection<int> TaskExecutionAll0 = new[]
        {
            TaskStarted,
            TaskFinished,
            TaskFailed,
            TaskTimedout,
            TaskSessionAttrSet,
            TaskReduced
        }.AsReadOnly();

        /// <summary>
        /// All cache events.
        /// </summary>
        private static readonly ICollection<int> CacheAll0 = new[]
        {
            CacheEntryCreated,
            CacheEntryDestroyed,
            CacheObjectPut,
            CacheObjectRead,
            CacheObjectRemoved,
            CacheObjectLocked,
            CacheObjectUnlocked,
            CacheObjectExpired
        }.AsReadOnly();

        /// <summary>
        /// All cache rebalance events.
        /// </summary>
        private static readonly ICollection<int> CacheRebalanceAll0 = new[]
        {
            CacheRebalanceStarted,
            CacheRebalanceStopped,
            CacheRebalancePartLoaded,
            CacheRebalancePartUnloaded,
            CacheRebalanceObjectLoaded,
            CacheRebalanceObjectUnloaded,
            CacheRebalancePartDataLost
        }.AsReadOnly();

        /// <summary>
        /// All cache lifecycle events.
        /// </summary>
        private static readonly ICollection<int> CacheLifecycleAll0 = new[]
        {
            CacheStarted,
            CacheStopped,
            CacheNodesLeft
        }.AsReadOnly();

        /// <summary>
        /// All cache query events.
        /// </summary>
        private static readonly ICollection<int> CacheQueryAll0 = new[]
        {
            CacheQueryExecuted,
            CacheQueryObjectRead
        }.AsReadOnly();

        /// <summary>
        /// All Ignite events.
        /// </summary>
        private static readonly ICollection<int> All0 = GetAllEvents().AsReadOnly();

        /// <summary>
        /// All Ignite events (<b>excluding</b> metric update event).
        /// </summary>
        private static readonly ICollection<int> AllMinusMetricUpdate0 =
            All0.Where(x => x != NodeMetricsUpdated).ToArray().AsReadOnly();

        /// <summary>
        /// All events indicating an error or failure condition. It is convenient to use when fetching all events 
        /// indicating error or failure.
        /// </summary>
        public static ICollection<int> ErrorAll
        {
            get { return ErrorAll0; }
        }

        /// <summary>
        /// All Ignite events (<b>excluding</b> metric update event).
        /// </summary>
        public static ICollection<int> AllMinusMetricUpdate
        {
            get { return AllMinusMetricUpdate0; }
        }

        /// <summary>
        /// All cache query events.
        /// </summary>
        public static ICollection<int> CacheQueryAll
        {
            get { return CacheQueryAll0; }
        }

        /// <summary>
        /// All cache lifecycle events.
        /// </summary>
        public static ICollection<int> CacheLifecycleAll
        {
            get { return CacheLifecycleAll0; }
        }

        /// <summary>
        /// All cache rebalance events.
        /// </summary>
        public static ICollection<int> CacheRebalanceAll
        {
            get { return CacheRebalanceAll0; }
        }

        /// <summary>
        /// All cache events.
        /// </summary>
        public static ICollection<int> CacheAll
        {
            get { return CacheAll0; }
        }

        /// <summary>
        /// All Ignite task execution events.
        /// </summary>
        public static ICollection<int> TaskExecutionAll
        {
            get { return TaskExecutionAll0; }
        }

        /// <summary>
        /// All Ignite job execution events.
        /// </summary>
        public static ICollection<int> JobExecutionAll
        {
            get { return JobExecutionAll0; }
        }

        /// <summary>
        /// All discovery events.
        /// </summary>
        public static ICollection<int> DiscoveryAll
        {
            get { return DiscoveryAll0; }
        }

        /// <summary>
        /// All discovery events except for <see cref="NodeMetricsUpdated" />. Subscription to <see 
        /// cref="NodeMetricsUpdated" /> can generate massive amount of event processing in most cases is not 
        /// necessary. If this event is indeed required you can subscribe to it individually or use <see 
        /// cref="DiscoveryAll0" /> array.
        /// </summary>
        public static ICollection<int> DiscoveryAllMinusMetrics
        {
            get { return DiscoveryAllMinusMetrics0; }
        }

        /// <summary>
        /// All Ignite events.
        /// </summary>
        public static ICollection<int> All
        {
            get { return All0; }
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