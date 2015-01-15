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

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.streamer.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Streamer execution future.
 */
public class GridStreamerStageExecutionFuture extends GridFutureAdapter<Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger. */
    private IgniteLogger log;

    /** Execution ID. */
    private final IgniteUuid execId;

    /** Execution start timestamp. */
    private final long execStartTs;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Parent future ID. By the contract, global ID is sender node ID. */
    private final IgniteUuid parentFutId;

    /** Stage name. */
    private final String stageName;

    /** Events. */
    private final Collection<Object> evts;

    /** Failover attempts count. */
    private int failoverAttemptCnt;

    /** Child executions. */
    private final ConcurrentMap<UUID, GridStreamerExecutionBatch> childExecs = new ConcurrentHashMap<>();

    /** Nodes on which this pipeline is known to be executed. */
    private final Set<UUID> execNodeIds = new GridConcurrentHashSet<>();

    /** Streamer context. */
    @GridToStringExclude
    private final IgniteStreamerEx streamer;

    /** Metrics holder. */
    @GridToStringExclude
    private StreamerMetricsHolder metricsHolder;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridStreamerStageExecutionFuture() {
        assert false : "Streamer execution future should never be serialized.";

        execId = null;
        execStartTs = 0;
        futId = null;
        stageName = null;
        evts = null;
        streamer = null;
        parentFutId = null;
    }

    /**
    * @param streamer Streamer extended context.
    * @param execId Execution ID. If parent future ID is {@code null} then this is a root future
    * and execution ID must be {@code null}.
     * @param failoverAttemptCnt Number of attempts this set of events was tried to failover.
    * @param execStartTs Execution start timestamp.
    * @param parentFutId Parent future ID.
    * @param prevExecNodes Node IDs on which pipeline was already executed.
    * @param stageName Stage name to run.
    * @param evts Events to process.
    */
    public GridStreamerStageExecutionFuture(
        IgniteStreamerEx streamer,
        @Nullable IgniteUuid execId,
        int failoverAttemptCnt,
        long execStartTs,
        @Nullable IgniteUuid parentFutId,
        @Nullable Collection<UUID> prevExecNodes,
        String stageName,
        Collection<?> evts
    ) {
        super(streamer.kernalContext());

        assert streamer != null;
        assert stageName != null;
        assert evts != null;
        assert !evts.isEmpty();
        assert (execId == null && parentFutId == null) || (execId != null && parentFutId != null);

        this.streamer = streamer;
        futId = IgniteUuid.fromUuid(streamer.kernalContext().localNodeId());
        this.parentFutId = parentFutId;

        this.execId = parentFutId == null ? futId : execId;
        this.failoverAttemptCnt = failoverAttemptCnt;
        this.execStartTs = execStartTs;

        this.stageName = stageName;
        this.evts = (Collection<Object>)evts;

        if (prevExecNodes != null)
            execNodeIds.addAll(prevExecNodes);

        log = streamer.kernalContext().log(GridStreamerStageExecutionFuture.class);
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid id() {
        return futId;
    }

    /**
     * Sets metrics holder to update counters when future completes. Used to avoid unnecessary listener creation.
     *
     * @param metricsHolder Metrics holder.
     */
    public void metrics(StreamerMetricsHolder metricsHolder) {
        assert metricsHolder != null;
        assert rootExecution();

        this.metricsHolder = metricsHolder;
    }

    /**
     * @return Failover attempt count.
     */
    public int failoverAttemptCount() {
        return failoverAttemptCnt;
    }

    /**
     * @return Stage name.
     */
    public String stageName() {
        return stageName;
    }

    /**
     * @return Events collection.
     */
    public Collection<Object> events() {
        return evts;
    }

    /**
     * Sends execution requests to remote nodes or schedules local execution if events were mapped locally.
     */
    public void map() {
        try {
            // This will be a no-op when atLeastOnce is false, so this future will be discarded right
            // after map() is executed.
            streamer.onFutureMapped(this);

            StreamerEventRouter evtRouter = streamer.eventRouter();

            Map<ClusterNode, Collection<Object>> routeMap = evtRouter.route(streamer.context(), stageName, evts);

            if (log.isDebugEnabled())
                log.debug("Mapped stage to nodes [futId=" + futId + ", stageName=" + stageName +
                    ", nodeIds=" + (routeMap != null ? U.nodeIds(routeMap.keySet()) : null) + ']');

            if (F.isEmpty(routeMap)) {
                U.error(log, "Failed to route events to nodes (will fail pipeline execution) " +
                    "[streamer=" + streamer.name() + ", stageName=" + stageName + ", evts=" + evts + ']');

                UUID locNodeId = streamer.kernalContext().localNodeId();

                onFailed(locNodeId, new GridStreamerRouteFailedException("Failed to route " +
                    "events to nodes (router returned null or empty route map) [locNodeId=" + locNodeId + ", " +
                    "stageName=" + stageName + ']'));
            }
            else {
                execNodeIds.addAll(U.nodeIds(routeMap.keySet()));

                for (Map.Entry<ClusterNode, Collection<Object>> entry : routeMap.entrySet()) {
                    ClusterNode node = entry.getKey();

                    childExecs.put(node.id(), new GridStreamerExecutionBatch(
                        execId,
                        execStartTs,
                        futId,
                        execNodeIds,
                        stageName,
                        entry.getValue()));
                }

                // Send execution requests to nodes.
                streamer.scheduleExecutions(this, childExecs);
            }
        }
        catch (IgniteCheckedException e) {
            onFailed(ctx.localNodeId(), e);
        }
    }

    /**
     * @return {@code True} if this future is a root execution future (i.e. initiated by streamer's addEvent call).
     */
    public boolean rootExecution() {
        return parentFutId == null;
    }

    /**
     * If not root future, will return parent node ID (may be local node ID).
     *
     * @return Sender node ID.
     */
    @Nullable public UUID senderNodeId() {
        return parentFutId == null ? null : parentFutId.globalId();
    }

    /**
     * If not root future, will return parent future ID.
     *
     * @return Parent future ID.
     */
    public IgniteUuid parentFutureId() {
        return parentFutId;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        return super.error();
    }

    /**
     * @return Map of child executions.
     */
    public Map<UUID, GridStreamerExecutionBatch> childExecutions() {
        return Collections.unmodifiableMap(childExecs);
    }

    /**
     * @return Execution node IDs.
     */
    public Collection<UUID> executionNodeIds() {
        return execNodeIds;
    }

    /**
     * Callback invoked when child node reports execution is completed (successfully or not).
     *
     * @param childNodeId Child node ID.
     * @param err Exception if execution failed.
     */
    public void onExecutionCompleted(UUID childNodeId, @Nullable Throwable err) {
        if (log.isDebugEnabled())
            log.debug("Completed child execution for node [fut=" + this +
                ", childNodeId=" + childNodeId + ", err=" + err + ']');

        if (err != null)
            onFailed(childNodeId, err);
        else {
            childExecs.remove(childNodeId);

            if (childExecs.isEmpty())
                onDone();
        }
    }

    /**
     * Callback invoked when node leaves the grid. If left node is known to participate in
     * pipeline execution, cancel all locally running stages.
     *
     * @param leftNodeId Node ID that has left the grid.
     */
    public void onNodeLeft(UUID leftNodeId) {
        if (execNodeIds.contains(leftNodeId))
            onFailed(leftNodeId, new ClusterTopologyException("Failed to wait for streamer pipeline future completion " +
                "(execution node has left the grid). All running stages will be cancelled " +
                "[fut=" + this + ", leftNodeId=" + leftNodeId + ']'));
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            if (log.isDebugEnabled())
                log.debug("Completed stage execution future [fut=" + this + ", err=" + err + ']');

            if (rootExecution() && metricsHolder != null) {
                if (err != null)
                    metricsHolder.onSessionFinished();
                else
                    metricsHolder.onSessionFailed();
            }

            streamer.onFutureCompleted(this);

            return true;
        }

        return false;
    }

    /**
     * Failed callback.
     *
     * @param failedNodeId Failed node ID.
     * @param err Error reason.
     */
    private void onFailed(UUID failedNodeId, Throwable err) {
        if (log.isDebugEnabled())
            log.debug("Pipeline execution failed on node [fut=" + this + ", failedNodeId=" + failedNodeId +
                ", err=" + err + ']');

        onDone(err);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        if (!onCancelled())
            return false;

        if (log.isDebugEnabled())
            log.debug("Cancelling streamer execution future: " + this);

        streamer.onFutureCompleted(this);

        return true;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridStreamerStageExecutionFuture.class, this, "childNodes", childExecs.keySet());
    }
}
