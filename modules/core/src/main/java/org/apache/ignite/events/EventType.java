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

package org.apache.ignite.events;

import java.util.List;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Contains event type constants. The decision to use class and not enumeration
 * dictated by allowing users to create their own events and/or event types which
 * would be impossible with enumerations.
 * <p>
 * Note that this interface defines not only
 * individual type constants but arrays of types as well to be conveniently used with
 * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method:
 * <ul>
 * <li>{@link #EVTS_CACHE}</li>
 * <li>{@link #EVTS_CACHE_LIFECYCLE}</li>
 * <li>{@link #EVTS_CACHE_REBALANCE}</li>
 * <li>{@link #EVTS_CACHE_QUERY}</li>
 * <li>{@link #EVTS_CHECKPOINT}</li>
 * <li>{@link #EVTS_DEPLOYMENT}</li>
 * <li>{@link #EVTS_DISCOVERY}</li>
 * <li>{@link #EVTS_DISCOVERY_ALL}</li>
 * <li>{@link #EVTS_ERROR}</li>
 * <li>{@link #EVTS_IGFS}</li>
 * <li>{@link #EVTS_JOB_EXECUTION}</li>
 * <li>{@link #EVTS_SWAPSPACE}</li>
 * <li>{@link #EVTS_TASK_EXECUTION}</li>
 * </ul>
 * <p>
 * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
 * internal Ignite events and should not be used by user-defined events.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. Ignite can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using either {@link IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration.
 * Note that certain events are required for Ignite's internal operations and such events will still be
 * generated but not stored by event storage SPI if they are disabled in Ignite configuration.
 */
public interface EventType {
    /**
     * Built-in event type: checkpoint was saved.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CheckpointEvent
     */
    public static final int EVT_CHECKPOINT_SAVED = 1;

    /**
     * Built-in event type: checkpoint was loaded.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CheckpointEvent
     */
    public static final int EVT_CHECKPOINT_LOADED = 2;

    /**
     * Built-in event type: checkpoint was removed. Reasons are:
     * <ul>
     * <li>timeout expired, or
     * <li>or it was manually removed, or
     * <li>it was automatically removed by the task session
     * </ul>
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CheckpointEvent
     */
    public static final int EVT_CHECKPOINT_REMOVED = 3;

    /**
     * Built-in event type: node joined topology.
     * <br>
     * New node has been discovered and joined grid topology.
     * Note that even though a node has been discovered there could be
     * a number of warnings in the log. In certain situations Ignite
     * doesn't prevent a node from joining but prints warning messages into the log.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DiscoveryEvent
     */
    public static final int EVT_NODE_JOINED = 10;

    /**
     * Built-in event type: node has normally left topology.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DiscoveryEvent
     */
    public static final int EVT_NODE_LEFT = 11;

    /**
     * Built-in event type: node failed.
     * <br>
     * Ignite detected that node has presumably crashed and is considered failed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DiscoveryEvent
     */
    public static final int EVT_NODE_FAILED = 12;

    /**
     * Built-in event type: node metrics updated.
     * <br>
     * Generated when node's metrics are updated. In most cases this callback
     * is invoked with every heartbeat received from a node (including local node).
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DiscoveryEvent
     */
    public static final int EVT_NODE_METRICS_UPDATED = 13;

    /**
     * Built-in event type: local node segmented.
     * <br>
     * Generated when node determines that it runs in invalid network segment.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DiscoveryEvent
     */
    public static final int EVT_NODE_SEGMENTED = 14;

    /**
     * Built-in event type: client node disconnected.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DiscoveryEvent
     */
    public static final int EVT_CLIENT_NODE_DISCONNECTED = 16;

    /**
     * Built-in event type: client node reconnected.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DiscoveryEvent
     */
    public static final int EVT_CLIENT_NODE_RECONNECTED = 17;

    /**
     * Built-in event type: task started.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see TaskEvent
     */
    public static final int EVT_TASK_STARTED = 20;

    /**
     * Built-in event type: task finished.
     * <br>
     * Task got finished. This event is triggered every time
     * a task finished without exception.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see TaskEvent
     */
    public static final int EVT_TASK_FINISHED = 21;

    /**
     * Built-in event type: task failed.
     * <br>
     * Task failed. This event is triggered every time a task finished with an exception.
     * Note that prior to this event, there could be other events recorded specific
     * to the failure.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see TaskEvent
     */
    public static final int EVT_TASK_FAILED = 22;

    /**
     * Built-in event type: task timed out.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see TaskEvent
     */
    public static final int EVT_TASK_TIMEDOUT = 23;

    /**
     * Built-in event type: task session attribute set.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see TaskEvent
     */
    public static final int EVT_TASK_SESSION_ATTR_SET = 24;

    /**
     * Built-in event type: task reduced.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     */
    public static final int EVT_TASK_REDUCED = 25;

    /**
     * Built-in event type: non-task class deployed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DeploymentEvent
     */
    public static final int EVT_CLASS_DEPLOYED = 30;

    /**
     * Built-in event type: non-task class undeployed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DeploymentEvent
     */
    public static final int EVT_CLASS_UNDEPLOYED = 31;

    /**
     * Built-in event type: non-task class deployment failed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DeploymentEvent
     */
    public static final int EVT_CLASS_DEPLOY_FAILED = 32;

    /**
     * Built-in event type: task deployed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DeploymentEvent
     */
    public static final int EVT_TASK_DEPLOYED = 33;

    /**
     * Built-in event type: task undeployed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DeploymentEvent
     */
    public static final int EVT_TASK_UNDEPLOYED = 34;

    /**
     * Built-in event type: task deployment failed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see DeploymentEvent
     */
    public static final int EVT_TASK_DEPLOY_FAILED = 35;

    /**
     * Built-in event type: grid job was mapped in
     * {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_MAPPED = 40;

    /**
     * Built-in event type: grid job result was received by
     * {@link org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} method.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_RESULTED = 41;

    /**
     * Built-in event type: grid job failed over.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_FAILED_OVER = 43;

    /**
     * Built-in event type: grid job started.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_STARTED = 44;

    /**
     * Built-in event type: grid job finished.
     * <br>
     * Job has successfully completed and produced a result which from the user perspective
     * can still be either negative or positive.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_FINISHED = 45;

    /**
     * Built-in event type: grid job timed out.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_TIMEDOUT = 46;

    /**
     * Built-in event type: grid job rejected during collision resolution.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_REJECTED = 47;

    /**
     * Built-in event type: grid job failed.
     * <br>
     * Job has failed. This means that there was some error event during job execution
     * and job did not produce a result.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_FAILED = 48;

    /**
     * Built-in event type: grid job queued.
     * <br>
     * Job arrived for execution and has been queued (added to passive queue during
     * collision resolution).
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_QUEUED = 49;

    /**
     * Built-in event type: grid job cancelled.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see JobEvent
     */
    public static final int EVT_JOB_CANCELLED = 50;

    /**
     * Built-in event type: entry created.
     * <p/>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
     public static final int EVT_CACHE_ENTRY_CREATED = 60;

     /**
      * Built-in event type: entry destroyed.
      * <p>
      * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
      * internal Ignite events and should not be used by user-defined events.
      *
      * @see CacheEvent
      */
     public static final int EVT_CACHE_ENTRY_DESTROYED = 61;

    /**
     * Built-in event type: entry evicted.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
     public static final int EVT_CACHE_ENTRY_EVICTED = 62;

     /**
      * Built-in event type: object put.
      * <p>
      * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
      * internal Ignite events and should not be used by user-defined events.
      *
      * @see CacheEvent
      */
     public static final int EVT_CACHE_OBJECT_PUT = 63;

     /**
      * Built-in event type: object read.
      * <p>
      * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
      * internal Ignite events and should not be used by user-defined events.
      *
      * @see CacheEvent
      */
     public static final int EVT_CACHE_OBJECT_READ = 64;

     /**
      * Built-in event type: object removed.
      * <p>
      * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
      * internal Ignite events and should not be used by user-defined events.
      *
      * @see CacheEvent
      */
     public static final int EVT_CACHE_OBJECT_REMOVED = 65;

     /**
      * Built-in event type: object locked.
      * <p>
      * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
      * internal Ignite events and should not be used by user-defined events.
      *
      * @see CacheEvent
      */
     public static final int EVT_CACHE_OBJECT_LOCKED = 66;

     /**
      * Built-in event type: object unlocked.
      * <p>
      * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
      * internal Ignite events and should not be used by user-defined events.
      *
      * @see CacheEvent
      */
     public static final int EVT_CACHE_OBJECT_UNLOCKED = 67;

    /**
     * Built-in event type: cache object swapped from swap storage.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_OBJECT_SWAPPED = 68;

    /**
     * Built-in event type: cache object unswapped from swap storage.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_OBJECT_UNSWAPPED = 69;

    /**
     * Built-in event type: cache object was expired when reading it.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_OBJECT_EXPIRED = 70;

    /**
     * Built-in event type: swap space data read.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see SwapSpaceEvent
     */
    public static final int EVT_SWAP_SPACE_DATA_READ = 71;

    /**
     * Built-in event type: swap space data stored.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see SwapSpaceEvent
     */
    public static final int EVT_SWAP_SPACE_DATA_STORED = 72;

    /**
     * Built-in event type: swap space data removed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see SwapSpaceEvent
     */
    public static final int EVT_SWAP_SPACE_DATA_REMOVED = 73;

    /**
     * Built-in event type: swap space cleared.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see SwapSpaceEvent
     */
    public static final int EVT_SWAP_SPACE_CLEARED = 74;

    /**
     * Built-in event type: swap space data evicted.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see SwapSpaceEvent
     */
    public static final int EVT_SWAP_SPACE_DATA_EVICTED = 75;

    /**
     * Built-in event type: cache object stored in off-heap storage.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_OBJECT_TO_OFFHEAP = 76;

    /**
     * Built-in event type: cache object moved from off-heap storage back into memory.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_OBJECT_FROM_OFFHEAP = 77;

    /**
     * Built-in event type: cache rebalance started.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheRebalancingEvent
     */
    public static final int EVT_CACHE_REBALANCE_STARTED = 80;

    /**
     * Built-in event type: cache rebalance stopped.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheRebalancingEvent
     */
    public static final int EVT_CACHE_REBALANCE_STOPPED = 81;

    /**
     * Built-in event type: cache partition loaded.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheRebalancingEvent
     */
    public static final int EVT_CACHE_REBALANCE_PART_LOADED = 82;

    /**
     * Built-in event type: cache partition unloaded.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheRebalancingEvent
     */
    public static final int EVT_CACHE_REBALANCE_PART_UNLOADED = 83;

    /**
     * Built-in event type: cache entry rebalanced.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_REBALANCE_OBJECT_LOADED = 84;

    /**
     * Built-in event type: cache entry unloaded.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_REBALANCE_OBJECT_UNLOADED = 85;

    /**
     * Built-in event type: all nodes that hold partition left topology.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheRebalancingEvent
     */
    public static final int EVT_CACHE_REBALANCE_PART_DATA_LOST = 86;

    /**
     * Built-in event type: query executed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheQueryExecutedEvent
     */
    public static final int EVT_CACHE_QUERY_EXECUTED = 96;

    /**
     * Built-in event type: query entry read.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheQueryExecutedEvent
     */
    public static final int EVT_CACHE_QUERY_OBJECT_READ = 97;

    /**
     * Built-in event type: cache started.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_STARTED = 98;

    /**
     * Built-in event type: cache started.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_STOPPED = 99;

    /**
     * Built-in event type: cache nodes left.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see CacheEvent
     */
    public static final int EVT_CACHE_NODES_LEFT = 100;

    /**
     * Built-in event type: IGFS file created.
     * <p>
     * Fired when IGFS component creates new file.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_CREATED = 116;

    /**
     * Built-in event type: IGFS file renamed.
     * <p>
     * Fired when IGFS component renames an existing file.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_RENAMED = 117;

    /**
     * Built-in event type: IGFS file deleted.
     * <p>
     * Fired when IGFS component deletes a file.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_DELETED = 118;

    /**
     * Built-in event type: IGFS file opened for reading.
     * <p>
     * Fired when IGFS file is opened for reading.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_OPENED_READ = 119;

    /**
     * Built-in event type: IGFS file opened for writing.
     * <p>
     * Fired when IGFS file is opened for writing.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_OPENED_WRITE = 120;

    /**
     * Built-in event type: IGFS file or directory metadata updated.
     * <p>
     * Fired when IGFS file or directory metadata is updated.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_META_UPDATED = 121;

    /**
     * Built-in event type: IGFS file closed.
     * <p>
     * Fired when IGFS file is closed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_CLOSED_WRITE = 122;

    /**
     * Built-in event type: IGFS file closed.
     * <p>
     * Fired when IGFS file is closed.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_CLOSED_READ = 123;

    /**
     * Built-in event type: IGFS directory created.
     * <p>
     * Fired when IGFS component creates new directory.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_DIR_CREATED = 124;

    /**
     * Built-in event type: IGFS directory renamed.
     * <p>
     * Fired when IGFS component renames an existing directory.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_DIR_RENAMED = 125;

    /**
     * Built-in event type: IGFS directory deleted.
     * <p>
     * Fired when IGFS component deletes a directory.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_DIR_DELETED = 126;

    /**
     * Built-in event type: IGFS file purged.
     * <p>
     * Fired when IGFS file data was actually removed from cache.
     * <p>
     * NOTE: all types in range <b>from 1 to 1000 are reserved</b> for
     * internal Ignite events and should not be used by user-defined events.
     *
     * @see IgfsEvent
     */
    public static final int EVT_IGFS_FILE_PURGED = 127;

    /**
     * All checkpoint events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all checkpoint events.
     *
     * @see CheckpointEvent
     */
    public static final int[] EVTS_CHECKPOINT = {
        EVT_CHECKPOINT_SAVED,
        EVT_CHECKPOINT_LOADED,
        EVT_CHECKPOINT_REMOVED
    };

    /**
     * All deployment events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all deployment events.
     *
     * @see DeploymentEvent
     */
    public static final int[] EVTS_DEPLOYMENT = {
        EVT_CLASS_DEPLOYED,
        EVT_CLASS_UNDEPLOYED,
        EVT_CLASS_DEPLOY_FAILED,
        EVT_TASK_DEPLOYED,
        EVT_TASK_UNDEPLOYED,
        EVT_TASK_DEPLOY_FAILED
    };

    /**
     * All events indicating an error or failure condition. It is convenient to use
     * when fetching all events indicating error or failure.
     */
    public static final int[] EVTS_ERROR = {
        EVT_JOB_TIMEDOUT,
        EVT_JOB_FAILED,
        EVT_JOB_FAILED_OVER,
        EVT_JOB_REJECTED,
        EVT_JOB_CANCELLED,
        EVT_TASK_TIMEDOUT,
        EVT_TASK_FAILED,
        EVT_CLASS_DEPLOY_FAILED,
        EVT_TASK_DEPLOY_FAILED,
        EVT_TASK_DEPLOYED,
        EVT_TASK_UNDEPLOYED,
        EVT_CACHE_REBALANCE_STARTED,
        EVT_CACHE_REBALANCE_STOPPED
    };

    /**
     * All discovery events <b>except</b> for {@link #EVT_NODE_METRICS_UPDATED}. Subscription to
     * {@link #EVT_NODE_METRICS_UPDATED} can generate massive amount of event processing in most cases
     * is not necessary. If this event is indeed required you can subscribe to it individually or use
     * {@link #EVTS_DISCOVERY_ALL} array.
     * <p>
     * This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all discovery events <b>except</b> for {@link #EVT_NODE_METRICS_UPDATED}.
     *
     * @see DiscoveryEvent
     */
    public static final int[] EVTS_DISCOVERY = {
        EVT_NODE_JOINED,
        EVT_NODE_LEFT,
        EVT_NODE_FAILED,
        EVT_NODE_SEGMENTED,
        EVT_CLIENT_NODE_DISCONNECTED,
        EVT_CLIENT_NODE_RECONNECTED
    };

    /**
     * All discovery events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all discovery events.
     *
     * @see DiscoveryEvent
     */
    public static final int[] EVTS_DISCOVERY_ALL = {
        EVT_NODE_JOINED,
        EVT_NODE_LEFT,
        EVT_NODE_FAILED,
        EVT_NODE_SEGMENTED,
        EVT_NODE_METRICS_UPDATED,
        EVT_CLIENT_NODE_DISCONNECTED,
        EVT_CLIENT_NODE_RECONNECTED
    };

    /**
     * All grid job execution events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all grid job execution events.
     *
     * @see JobEvent
     */
    public static final int[] EVTS_JOB_EXECUTION = {
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

    /**
     * All grid task execution events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all grid task execution events.
     *
     * @see TaskEvent
     */
    public static final int[] EVTS_TASK_EXECUTION = {
        EVT_TASK_STARTED,
        EVT_TASK_FINISHED,
        EVT_TASK_FAILED,
        EVT_TASK_TIMEDOUT,
        EVT_TASK_SESSION_ATTR_SET,
        EVT_TASK_REDUCED
    };

    /**
     * All cache events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all cache events.
     */
    public static final int[] EVTS_CACHE = {
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

    /**
     * All cache rebalance events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all cache rebalance events.
     */
    public static final int[] EVTS_CACHE_REBALANCE = {
        EVT_CACHE_REBALANCE_STARTED,
        EVT_CACHE_REBALANCE_STOPPED,
        EVT_CACHE_REBALANCE_PART_LOADED,
        EVT_CACHE_REBALANCE_PART_UNLOADED,
        EVT_CACHE_REBALANCE_OBJECT_LOADED,
        EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
        EVT_CACHE_REBALANCE_PART_DATA_LOST
    };

    /**
     * All cache lifecycle events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all cache lifecycle events.
     */
    public static final int[] EVTS_CACHE_LIFECYCLE = {
        EVT_CACHE_STARTED,
        EVT_CACHE_STOPPED,
        EVT_CACHE_NODES_LEFT
    };

    /**
     * All cache query events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all cache query events.
     */
    public static final int[] EVTS_CACHE_QUERY = {
        EVT_CACHE_QUERY_EXECUTED,
        EVT_CACHE_QUERY_OBJECT_READ
    };

    /**
     * All swap space events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all cloud events.
     *
     * @see SwapSpaceEvent
     */
    public static final int[] EVTS_SWAPSPACE = {
        EVT_SWAP_SPACE_CLEARED,
        EVT_SWAP_SPACE_DATA_REMOVED,
        EVT_SWAP_SPACE_DATA_READ,
        EVT_SWAP_SPACE_DATA_STORED,
        EVT_SWAP_SPACE_DATA_EVICTED
    };

    /**
     * All Igfs events. This array can be directly passed into
     * {@link IgniteEvents#localListen(IgnitePredicate, int...)} method to
     * subscribe to all cloud events.
     *
     * @see IgfsEvent
     */
    public static final int[] EVTS_IGFS = {
        EVT_IGFS_FILE_CREATED,
        EVT_IGFS_FILE_RENAMED,
        EVT_IGFS_FILE_DELETED,
        EVT_IGFS_FILE_OPENED_READ,
        EVT_IGFS_FILE_OPENED_WRITE,
        EVT_IGFS_FILE_CLOSED_WRITE,
        EVT_IGFS_FILE_CLOSED_READ,
        EVT_IGFS_FILE_PURGED,
        EVT_IGFS_META_UPDATED,
        EVT_IGFS_DIR_CREATED,
        EVT_IGFS_DIR_RENAMED,
        EVT_IGFS_DIR_DELETED,
    };

    /**
     * All Ignite events (<b>including</b> metric update event).
     */
    public static final int[] EVTS_ALL = U.gridEvents();

    /**
     * All Ignite events (<b>excluding</b> metric update event).
     */
    public static final int[] EVTS_ALL_MINUS_METRIC_UPDATE = U.gridEvents(EVT_NODE_METRICS_UPDATED);
}