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

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Shuffle.
 */
public class GridHadoopShuffle extends GridHadoopComponent {
    /** */
    private final ConcurrentMap<GridHadoopJobId, GridHadoopShuffleJob<UUID>> jobs = new ConcurrentHashMap<>();

    /** */
    protected final GridUnsafeMemory mem = new GridUnsafeMemory(0);

    /** {@inheritDoc} */
    @Override public void start(GridHadoopContext ctx) throws IgniteCheckedException {
        super.start(ctx);

        ctx.kernalContext().io().addUserMessageListener(GridTopic.TOPIC_HADOOP,
            new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID nodeId, Object msg) {
                    return onMessageReceived(nodeId, (GridHadoopMessage)msg);
                }
            });
    }

    /**
     * Stops shuffle.
     *
     * @param cancel If should cancel all ongoing activities.
     */
    @Override public void stop(boolean cancel) {
        for (GridHadoopShuffleJob job : jobs.values()) {
            try {
                job.close();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to close job.", e);
            }
        }

        jobs.clear();
    }

    /**
     * Creates new shuffle job.
     *
     * @param jobId Job ID.
     * @return Created shuffle job.
     * @throws IgniteCheckedException If job creation failed.
     */
    private GridHadoopShuffleJob<UUID> newJob(GridHadoopJobId jobId) throws IgniteCheckedException {
        GridHadoopMapReducePlan plan = ctx.jobTracker().plan(jobId);

        GridHadoopShuffleJob<UUID> job = new GridHadoopShuffleJob<>(ctx.localNodeId(), log,
            ctx.jobTracker().job(jobId, null), mem, plan.reducers(), plan.reducers(ctx.localNodeId()));

        UUID[] rdcAddrs = new UUID[plan.reducers()];

        for (int i = 0; i < rdcAddrs.length; i++) {
            UUID nodeId = plan.nodeForReducer(i);

            assert nodeId != null : "Plan is missing node for reducer [plan=" + plan + ", rdc=" + i + ']';

            rdcAddrs[i] = nodeId;
        }

        boolean init = job.initializeReduceAddresses(rdcAddrs);

        assert init;

        return job;
    }

    /**
     * @param nodeId Node ID to send message to.
     * @param msg Message to send.
     * @throws IgniteCheckedException If send failed.
     */
    private void send0(UUID nodeId, Object msg) throws IgniteCheckedException {
        ClusterNode node = ctx.kernalContext().discovery().node(nodeId);

        ctx.kernalContext().io().sendUserMessage(F.asList(node), msg, GridTopic.TOPIC_HADOOP, false, 0);
    }

    /**
     * @param jobId Task info.
     * @return Shuffle job.
     */
    private GridHadoopShuffleJob<UUID> job(GridHadoopJobId jobId) throws IgniteCheckedException {
        GridHadoopShuffleJob<UUID> res = jobs.get(jobId);

        if (res == null) {
            res = newJob(jobId);

            GridHadoopShuffleJob<UUID> old = jobs.putIfAbsent(jobId, res);

            if (old != null) {
                res.close();

                res = old;
            }
            else if (res.reducersInitialized())
                startSending(res);
        }

        return res;
    }

    /**
     * Starts message sending thread.
     *
     * @param shuffleJob Job to start sending for.
     */
    private void startSending(GridHadoopShuffleJob<UUID> shuffleJob) {
        shuffleJob.startSending(ctx.kernalContext().gridName(),
            new IgniteInClosure2X<UUID, GridHadoopShuffleMessage>() {
                @Override public void applyx(UUID dest, GridHadoopShuffleMessage msg) throws IgniteCheckedException {
                    send0(dest, msg);
                }
            }
        );
    }

    /**
     * Message received callback.
     *
     * @param src Sender node ID.
     * @param msg Received message.
     * @return {@code True}.
     */
    public boolean onMessageReceived(UUID src, GridHadoopMessage msg) {
        if (msg instanceof GridHadoopShuffleMessage) {
            GridHadoopShuffleMessage m = (GridHadoopShuffleMessage)msg;

            try {
                job(m.jobId()).onShuffleMessage(m);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Message handling failed.", e);
            }

            try {
                // Reply with ack.
                send0(src, new GridHadoopShuffleAck(m.id(), m.jobId()));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to reply back to shuffle message sender [snd=" + src + ", msg=" + msg + ']', e);
            }
        }
        else if (msg instanceof GridHadoopShuffleAck) {
            GridHadoopShuffleAck m = (GridHadoopShuffleAck)msg;

            try {
                job(m.jobId()).onShuffleAck(m);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Message handling failed.", e);
            }
        }
        else
            throw new IllegalStateException("Unknown message type received to Hadoop shuffle [src=" + src +
                ", msg=" + msg + ']');

        return true;
    }

    /**
     * @param taskCtx Task info.
     * @return Output.
     */
    public GridHadoopTaskOutput output(GridHadoopTaskContext taskCtx) throws IgniteCheckedException {
        return job(taskCtx.taskInfo().jobId()).output(taskCtx);
    }

    /**
     * @param taskCtx Task info.
     * @return Input.
     */
    public GridHadoopTaskInput input(GridHadoopTaskContext taskCtx) throws IgniteCheckedException {
        return job(taskCtx.taskInfo().jobId()).input(taskCtx);
    }

    /**
     * @param jobId Job id.
     */
    public void jobFinished(GridHadoopJobId jobId) {
        GridHadoopShuffleJob job = jobs.remove(jobId);

        if (job != null) {
            try {
                job.close();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to close job: " + jobId, e);
            }
        }
    }

    /**
     * Flushes all the outputs for the given job to remote nodes.
     *
     * @param jobId Job ID.
     * @return Future.
     */
    public IgniteFuture<?> flush(GridHadoopJobId jobId) {
        GridHadoopShuffleJob job = jobs.get(jobId);

        if (job == null)
            return new GridFinishedFutureEx<>();

        try {
            return job.flush();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFutureEx<>(e);
        }
    }

    /**
     * @return Memory.
     */
    public GridUnsafeMemory memory() {
        return mem;
    }
}
