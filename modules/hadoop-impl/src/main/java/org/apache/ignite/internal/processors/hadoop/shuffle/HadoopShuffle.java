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

package org.apache.ignite.internal.processors.hadoop.shuffle;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopComponent;
import org.apache.ignite.internal.processors.hadoop.HadoopContext;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Shuffle.
 */
public class HadoopShuffle extends HadoopComponent {
    /** */
    private final ConcurrentMap<HadoopJobId, HadoopShuffleJob<UUID>> jobs = new ConcurrentHashMap<>();

    /** */
    protected final GridUnsafeMemory mem = new GridUnsafeMemory(0);

    /** {@inheritDoc} */
    @Override public void start(HadoopContext ctx) throws IgniteCheckedException {
        super.start(ctx);

        ctx.kernalContext().io().addUserMessageListener(GridTopic.TOPIC_HADOOP,
            new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID nodeId, Object msg) {
                    return onMessageReceived(nodeId, (HadoopMessage)msg);
                }
            });
    }

    /**
     * Stops shuffle.
     *
     * @param cancel If should cancel all ongoing activities.
     */
    @Override public void stop(boolean cancel) {
        for (HadoopShuffleJob job : jobs.values()) {
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
    private HadoopShuffleJob<UUID> newJob(HadoopJobId jobId) throws IgniteCheckedException {
        HadoopMapReducePlan plan = ctx.jobTracker().plan(jobId);

        HadoopShuffleJob<UUID> job = new HadoopShuffleJob<>(ctx.localNodeId(), log,
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
    private HadoopShuffleJob<UUID> job(HadoopJobId jobId) throws IgniteCheckedException {
        HadoopShuffleJob<UUID> res = jobs.get(jobId);

        if (res == null) {
            res = newJob(jobId);

            HadoopShuffleJob<UUID> old = jobs.putIfAbsent(jobId, res);

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
    private void startSending(HadoopShuffleJob<UUID> shuffleJob) {
        shuffleJob.startSending(ctx.kernalContext().gridName(),
            new IgniteInClosure2X<UUID, HadoopShuffleMessage>() {
                @Override public void applyx(UUID dest, HadoopShuffleMessage msg) throws IgniteCheckedException {
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
    public boolean onMessageReceived(UUID src, HadoopMessage msg) {
        if (msg instanceof HadoopShuffleMessage) {
            HadoopShuffleMessage m = (HadoopShuffleMessage)msg;

            try {
                job(m.jobId()).onShuffleMessage(m);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Message handling failed.", e);
            }

            try {
                // Reply with ack.
                send0(src, new HadoopShuffleAck(m.id(), m.jobId()));
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to reply back to shuffle message sender [snd=" + src + ", msg=" + msg + ']', e);
            }
        }
        else if (msg instanceof HadoopShuffleAck) {
            HadoopShuffleAck m = (HadoopShuffleAck)msg;

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
    public HadoopTaskOutput output(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        return job(taskCtx.taskInfo().jobId()).output(taskCtx);
    }

    /**
     * @param taskCtx Task info.
     * @return Input.
     */
    public HadoopTaskInput input(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        return job(taskCtx.taskInfo().jobId()).input(taskCtx);
    }

    /**
     * @param jobId Job id.
     */
    public void jobFinished(HadoopJobId jobId) {
        HadoopShuffleJob job = jobs.remove(jobId);

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
    public IgniteInternalFuture<?> flush(HadoopJobId jobId) {
        HadoopShuffleJob job = jobs.get(jobId);

        if (job == null)
            return new GridFinishedFuture<>();

        try {
            return job.flush();
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @return Memory.
     */
    public GridUnsafeMemory memory() {
        return mem;
    }
}