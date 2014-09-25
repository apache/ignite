/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.lang.*;
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
    @Override public void start(GridHadoopContext ctx) throws GridException {
        super.start(ctx);

        ctx.kernalContext().io().addUserMessageListener(GridTopic.TOPIC_HADOOP,
            new GridBiPredicate<UUID, Object>() {
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
            catch (GridException e) {
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
     * @throws GridException If job creation failed.
     */
    private GridHadoopShuffleJob<UUID> newJob(GridHadoopJobId jobId) throws GridException {
        GridHadoopMapReducePlan plan = ctx.jobTracker().plan(jobId);

        GridHadoopShuffleJob<UUID> job = new GridHadoopShuffleJob<>(ctx.localNodeId(), log,
            ctx.jobTracker().job(jobId, null), ctx.jobTracker().statistics(jobId), mem, plan.reducers(),
            plan.reducers(ctx.localNodeId()));

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
     * @throws GridException If send failed.
     */
    private void send0(UUID nodeId, Object msg) throws GridException {
        GridNode node = ctx.kernalContext().discovery().node(nodeId);

        ctx.kernalContext().io().sendUserMessage(F.asList(node), msg, GridTopic.TOPIC_HADOOP, false, 0);
    }

    /**
     * @param jobId Task info.
     * @return Shuffle job.
     */
    private GridHadoopShuffleJob<UUID> job(GridHadoopJobId jobId) throws GridException {
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
            new GridInClosure2X<UUID, GridHadoopShuffleMessage>() {
                @Override public void applyx(UUID dest, GridHadoopShuffleMessage msg) throws GridException {
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
            catch (GridException e) {
                U.error(log, "Message handling failed.", e);
            }

            try {
                // Reply with ack.
                send0(src, new GridHadoopShuffleAck(m.id(), m.jobId()));
            }
            catch (GridException e) {
                U.error(log, "Failed to reply back to shuffle message sender [snd=" + src + ", msg=" + msg + ']', e);
            }
        }
        else if (msg instanceof GridHadoopShuffleAck) {
            GridHadoopShuffleAck m = (GridHadoopShuffleAck)msg;

            try {
                job(m.jobId()).onShuffleAck(m);
            }
            catch (GridException e) {
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
    public GridHadoopTaskOutput output(GridHadoopTaskContext taskCtx) throws GridException {
        return job(taskCtx.taskInfo().jobId()).output(taskCtx);
    }

    /**
     * @param taskCtx Task info.
     * @return Input.
     */
    public GridHadoopTaskInput input(GridHadoopTaskContext taskCtx) throws GridException {
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
            catch (GridException e) {
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
    public GridFuture<?> flush(GridHadoopJobId jobId) {
        GridHadoopShuffleJob job = jobs.get(jobId);

        if (job == null)
            return new GridFinishedFutureEx<>();

        try {
            return job.flush();
        }
        catch (GridException e) {
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
