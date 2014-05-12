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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.*;

/**
 * Shuffle.
 */
public abstract class GridHadoopShuffleAdapter<T>  {
    /** */
    private ConcurrentMap<GridHadoopJobId, GridHadoopShuffleJob<T>> jobs = new ConcurrentHashMap<>();

    /** */
    protected GridUnsafeMemory mem = new GridUnsafeMemory(0);

    /** */
    protected ConcurrentMap<Long, GridBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>>> sentMsgs =
        new ConcurrentHashMap<>();

    /** Logger. */
    protected GridLogger log;

    /**
     * @param log Logger to use.
     */
    protected GridHadoopShuffleAdapter(GridLogger log) {
        this.log = log;
    }

    /**
     * @param dest Destination descriptor to send message to.
     * @param msg Message.
     */
    protected abstract void send0(T dest, Object msg);

    /**
     * Creates new shuffle job for this shuffle implementation.
     *
     * @param jobId Job ID to create shuffle for.
     * @return Shuffle job.
     * @throws GridException If Job creation failed.
     */
    protected abstract GridHadoopShuffleJob<T> newJob(GridHadoopJobId jobId) throws GridException;

    /**
     * Starts shuffle component.
     */
    protected abstract void start();

    /**
     * @return Grid name.
     */
    protected abstract String gridName();

    /**
     * @param jobId Task info.
     * @return Shuffle job.
     */
    private GridHadoopShuffleJob job(GridHadoopJobId jobId) throws GridException {
        GridHadoopShuffleJob<T> res = jobs.get(jobId);

        if (res == null) {
            res = newJob(jobId);

            GridHadoopShuffleJob<T> old = jobs.putIfAbsent(jobId, res);

            if (old != null) {
                res.close();

                res = old;
            }
            else {
                if (res.reducersInitialized()) {
                    res.startSending(gridName(),
                        new GridBiClosure<T, GridHadoopShuffleMessage, GridFuture<?>>() {
                            @Override public GridFuture<?> apply(T dest, GridHadoopShuffleMessage msg) {
                                GridFutureAdapterEx<?> f = new GridFutureAdapterEx<>();

                                sentMsgs.putIfAbsent(msg.id(),
                                    new GridBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>>(msg, f));

                                send0(dest, msg);

                                return f;
                            }
                        }
                    );
                }
            }
        }

        return res;
    }

    protected boolean onMessageReceived(T src, Object msg) {
        if (msg instanceof GridHadoopShuffleMessage) {
            GridHadoopShuffleMessage m = (GridHadoopShuffleMessage)msg;

            try {
                job(m.jobId()).onShuffleMessage(m);
            }
            catch (GridException e) {
                U.error(log, "Message handling failed.", e);
            }

            // Reply with ack.
            send0(src, new GridHadoopShuffleAck(m.id(), m.jobId()));
        }
        else if (msg instanceof GridHadoopShuffleAck) {
            GridHadoopShuffleAck m = (GridHadoopShuffleAck)msg;

            GridBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>> t = sentMsgs.remove(m.id());

            if (t != null)
                t.get2().onDone();
        }
        else
            throw new IllegalStateException("Unknown message type received to Hadoop shuffle [src=" + src +
                ", msg=" + msg + ']');

        return true;
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

    /**
     * Stops shuffle.
     *
     * @param cancel If should cancel all ongoing activities.
     */
    public void stop(boolean cancel) {
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
