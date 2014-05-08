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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.future.*;
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
    private ConcurrentMap<GridHadoopJobId, GridHadoopShuffleJob> jobs = new ConcurrentHashMap<>();

    /** */
    private GridUnsafeMemory mem = new GridUnsafeMemory(0);

    /** */
    private ConcurrentMap<Long, GridBiTuple<GridHadoopShuffleMessage, GridFutureAdapter<?>>> sentMsgs =
        new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        ctx.kernalContext().io().addUserMessageListener(GridTopic.TOPIC_HADOOP,
            new GridBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID nodeId, Object msg) {
                    if (msg instanceof GridHadoopShuffleMessage) {
                        GridHadoopShuffleMessage m = (GridHadoopShuffleMessage)msg;

                        try {
                            job(m.jobId()).onShuffleMessage(m);
                        }
                        catch (GridException e) {
                            U.error(log, "Message handling failed.", e);
                        }

                        // Reply with ack.
                        send0(nodeId, new GridHadoopShuffleAck(m.id(), m.jobId()));
                    }
                    else if (msg instanceof GridHadoopShuffleAck) {
                        GridHadoopShuffleAck m = (GridHadoopShuffleAck)msg;

                        GridBiTuple<GridHadoopShuffleMessage, GridFutureAdapter<?>> t = sentMsgs.remove(m.id());

                        if (t != null)
                            t.get2().onDone();
                    }
                    else
                        throw new IllegalStateException("Message unknown: " + msg);

                    return true;
                }
            });
    }

    /**
     * @param nodeId Node id.
     * @param msg Message.
     */
    private void send0(UUID nodeId, Object msg) {
        GridNode node = ctx.kernalContext().discovery().node(nodeId);

        try {
            ctx.kernalContext().io().sendUserMessage(F.asList(node), msg, GridTopic.TOPIC_HADOOP, false, 0);
        }
        catch (GridException e) {
            U.error(log, "Failed to send message.", e);
        }
    }

    /**
     * @param jobId Task info.
     * @return Shuffle job.
     */
    private GridHadoopShuffleJob job(GridHadoopJobId jobId) throws GridException {
        GridHadoopShuffleJob res = jobs.get(jobId);

        if (res == null) {
            res = new GridHadoopShuffleJob(ctx.localNodeId(), log, ctx.jobTracker().job(jobId), mem, ctx.jobTracker().plan(jobId));

            GridHadoopShuffleJob old = jobs.putIfAbsent(jobId, res);

            if (old != null) {
                res.close();

                res = old;
            }
            else {
                res.startSending(ctx.kernalContext().gridName(),
                    new GridBiClosure<UUID, GridHadoopShuffleMessage, GridFuture<?>>() {
                        @Override public GridFuture<?> apply(UUID nodeId, GridHadoopShuffleMessage msg) {
                            GridFutureAdapter<?> f = new GridFutureAdapter<>(ctx.kernalContext());

                            sentMsgs.putIfAbsent(msg.id(),
                                new GridBiTuple<GridHadoopShuffleMessage, GridFutureAdapter<?>>(msg, f));

                            send0(nodeId, msg);

                            return f;
                        }
                    }
                );
            }
        }

        return res;
    }

    /**
     * @param taskInfo Task info.
     * @return Output.
     */
    public GridHadoopTaskOutput output(GridHadoopTaskInfo taskInfo) throws GridException {
        return job(taskInfo.jobId()).output(taskInfo);
    }

    /**
     * @param taskInfo Task info.
     * @return Input.
     */
    public GridHadoopTaskInput input(GridHadoopTaskInfo taskInfo) throws GridException {
        return job(taskInfo.jobId()).input(taskInfo);
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

    /** {@inheritDoc} */
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
}
