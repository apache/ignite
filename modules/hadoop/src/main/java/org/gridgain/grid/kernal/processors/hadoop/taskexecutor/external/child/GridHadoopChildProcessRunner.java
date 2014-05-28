/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.child;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.hadoop.GridHadoopJobProperty.*;
import static org.gridgain.grid.hadoop.GridHadoopTaskType.*;

/**
 * Hadoop process base.
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public class GridHadoopChildProcessRunner {
    /** Node process descriptor. */
    private GridHadoopProcessDescriptor nodeDesc;

    /** Message processing executor service. */
    private ExecutorService msgExecSvc;

    /** Task executor service. */
    private ThreadPoolExecutor execSvc;

    /** */
    protected GridUnsafeMemory mem = new GridUnsafeMemory(0);

    /** External communication. */
    private GridHadoopExternalCommunication comm;

    /** Logger. */
    private GridLogger log;

    /** Init guard. */
    private AtomicBoolean initGuard = new AtomicBoolean();

    /** Start time. */
    private long startTime;

    /** Init future. */
    private GridFutureAdapterEx<?> initFut = new GridFutureAdapterEx<>();

    /** Job instance. */
    private GridHadoopJob job;

    /** Number of uncompleted tasks. */
    private AtomicInteger pendingTasks = new AtomicInteger();

    /** Job factory. */
    private GridHadoopJobFactory jobFactory = new GridHadoopDefaultJobFactory();

    /** Shuffle job. */
    private GridHadoopShuffleJob<GridHadoopProcessDescriptor> shuffleJob;

    /** Shared task context. */
    private GridHadoopJobClassLoadingContext clsLdrCtx;

    /** Concurrent mappers. */
    private int concMappers;

    /** Concurrent reducers. */
    private int concReducers;

    /**
     * Starts child process runner.
     */
    public void start(GridHadoopExternalCommunication comm, GridHadoopProcessDescriptor nodeDesc,
        ExecutorService msgExecSvc, GridLogger parentLog)
        throws GridException {
        this.comm = comm;
        this.nodeDesc = nodeDesc;
        this.msgExecSvc = msgExecSvc;

        comm.setListener(new MessageListener());
        log = parentLog.getLogger(GridHadoopChildProcessRunner.class);

        startTime = System.currentTimeMillis();

        // At this point node knows that this process has started.
        comm.sendMessage(this.nodeDesc, new GridHadoopProcessStartedAck());
    }

    /**
     * Initializes process for task execution.
     *
     * @param req Initialization request.
     */
    private void prepareProcess(GridHadoopPrepareForJobRequest req) {
        if (initGuard.compareAndSet(false, true)) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Initializing external hadoop task: " + req);

                job = jobFactory.createJob(req.jobId(), req.jobInfo());

                clsLdrCtx = new GridHadoopJobClassLoadingContext(nodeDesc.parentNodeId(), job, log);

                clsLdrCtx.initializeClassLoader();

                shuffleJob = new GridHadoopShuffleJob<>(comm.localProcessDescriptor(), log, job, mem, job.reducers(),
                    req.hasMappers());

                initializeExecutors(req);

                if (log.isDebugEnabled())
                    log.debug("External process initialized [initWaitTime=" +
                        (System.currentTimeMillis() - startTime) + ']');

                initFut.onDone(null, null);
            }
            catch (GridException e) {
                log.error("Failed to initialize process: " + req, e);

                initFut.onDone(e);
            }
        }
        else
            log.warning("Duplicate initialize process request received (will ignore): " + req);
    }

    /**
     * @param req Task execution request.
     */
    private void runTasks(final GridHadoopTaskExecutionRequest req) {
        if (!initFut.isDone() && log.isDebugEnabled())
            log.debug("Will wait for process initialization future completion: " + req);

        initFut.listenAsync(new CI1<GridFuture<?>>() {
            @Override public void apply(GridFuture<?> f) {
                try {
                    // Make sure init was successful.
                    f.get();

                    boolean set = pendingTasks.compareAndSet(0, req.tasks().size());

                    assert set;

                    GridHadoopTaskInfo info = F.first(req.tasks());

                    int size = info.type() == MAP ? concMappers : concReducers;

                    execSvc.setCorePoolSize(size);
                    execSvc.setMaximumPoolSize(size);

                    if (log.isDebugEnabled())
                        log.debug("Set executor service size for task type [type=" + info.type() +
                            ", size=" + size + ']');

                    for (GridHadoopTaskInfo taskInfo : req.tasks()) {
                        if (log.isDebugEnabled())
                            log.debug("Submitted task for external execution: " + taskInfo);

                        execSvc.submit(new GridHadoopRunnableTask(job, mem, taskInfo, clsLdrCtx) {
                            @Override protected void onTaskFinished(GridHadoopTaskState state, Throwable err) {
                                onTaskFinished0(this, state, err);
                            }

                            @Override protected GridHadoopTaskInput createInput(GridHadoopTaskInfo info)
                                throws GridException {
                                return shuffleJob.input(info);
                            }

                            @Override protected GridHadoopTaskOutput createOutput(GridHadoopTaskInfo info)
                                throws GridException {
                                return shuffleJob.output(info);
                            }
                        });
                    }
                }
                catch (GridException e) {
                    for (GridHadoopTaskInfo info : req.tasks())
                        notifyTaskFinished(info, GridHadoopTaskState.FAILED, e, false);
                }
            }
        });
    }

    /**
     * Creates executor services.
     *
     * @param req Init child process request.
     */
    private void initializeExecutors(GridHadoopPrepareForJobRequest req) {
        concMappers = Runtime.getRuntime().availableProcessors();
        concReducers = Runtime.getRuntime().availableProcessors();

        GridHadoopJobInfo info = req.jobInfo();

        if (info instanceof GridHadoopDefaultJobInfo) {
            GridHadoopDefaultJobInfo dfltInfo = (GridHadoopDefaultJobInfo)info;

            JobConf cfg = dfltInfo.configuration();

            concMappers = cfg.getInt(EXTERNAL_CONCURRENT_MAPPERS.propertyName(), concMappers);

            concReducers = cfg.getInt(EXTERNAL_CONCURRENT_REDUCERS.propertyName(), concReducers);
        }

        execSvc = new ThreadPoolExecutor(concMappers, concMappers,
            1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

    /**
     * Updates external process map so that shuffle can proceed with sending messages to reducers.
     *
     * @param req Update request.
     */
    private void updateTasks(final GridHadoopJobInfoUpdateRequest req) {
        initFut.listenAsync(new CI1<GridFuture<?>>() {
            @Override public void apply(GridFuture<?> gridFuture) {
                assert initGuard.get();

                assert req.jobId().equals(job.id());

                if (req.reducersAddresses() != null) {
                    if (shuffleJob.initializeReduceAddresses(req.reducersAddresses())) {
                        shuffleJob.startSending("external",
                            new GridInClosure2X<GridHadoopProcessDescriptor, GridHadoopShuffleMessage>() {
                                @Override public void applyx(GridHadoopProcessDescriptor dest,
                                    GridHadoopShuffleMessage msg) throws GridException {
                                    comm.sendMessage(dest, msg);
                                }
                            });
                    }
                }
            }
        });
    }

    /**
     * Stops all executors and running tasks.
     */
    private void shutdown() {
        if (execSvc != null)
            execSvc.shutdownNow();

        if (msgExecSvc != null)
            msgExecSvc.shutdownNow();
    }

    /**
     * Notifies node about task finish.
     *
     * @param run Finished task runnable.
     * @param state Task finish state.
     * @param err Error, if any.
     */
    private void onTaskFinished0(GridHadoopRunnableTask run, GridHadoopTaskState state, Throwable err) {
        GridHadoopTaskInfo info = run.taskInfo();

        int remainder = pendingTasks.decrementAndGet();

        if (log.isDebugEnabled())
            log.debug("Hadoop task execution finished [info=" + info
                + ", state=" + state + ", waitTime=" + run.waitTime() + ", execTime=" + run.executionTime() +
                ", pendingTasks=" + remainder +
                ", err=" + err + ']');

        boolean flush = false;

        if (remainder == 0 && (info.type() == COMBINE || (info.type() == MAP && !job.hasCombiner())))
            flush = true;

        notifyTaskFinished(info, state, err, flush);
    }

    /**
     * @param taskInfo Finished task info.
     * @param state Task finish state.
     * @param err Error, if any.
     */
    private void notifyTaskFinished(final GridHadoopTaskInfo taskInfo, final GridHadoopTaskState state,
        final Throwable err, boolean flush) {
        if (!flush) {
            try {
                if (log.isDebugEnabled())
                    log.debug("Sending notification to parent node [taskInfo=" + taskInfo + ", state=" + state +
                        ", err=" + err + ']');

                comm.sendMessage(nodeDesc, new GridHadoopTaskFinishedMessage(taskInfo, state, err));
            }
            catch (GridException e) {
                log.error("Failed to send message to parent node (will terminate child process).", e);

                shutdown();

                terminate();
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Flushing shuffle messages before sending last task completion notification [taskInfo=" +
                    taskInfo + ", state=" + state + ", err=" + err + ']');

            final long start = System.currentTimeMillis();

            try {
                shuffleJob.flush().listenAsync(new CI1<GridFuture<?>>() {
                    @Override public void apply(GridFuture<?> f) {
                        long end = System.currentTimeMillis();

                        if (log.isDebugEnabled())
                            log.debug("Finished flushing shuffle messages [taskInfo=" + taskInfo +
                                ", flushTime=" + (end - start) + ']');

                        try {
                            // Check for errors on shuffle.
                            f.get();

                            notifyTaskFinished(taskInfo, state, err, false);
                        }
                        catch (GridException e) {
                            log.error("Failed to flush shuffle messages (will fail the task) [taskInfo=" + taskInfo +
                                ", state=" + state + ", err=" + err + ']', e);

                            notifyTaskFinished(taskInfo, GridHadoopTaskState.FAILED, e, false);
                        }
                    }
                });
            }
            catch (GridException e) {
                log.error("Failed to flush shuffle messages (will fail the task) [taskInfo=" + taskInfo +
                    ", state=" + state + ", err=" + err + ']', e);

                notifyTaskFinished(taskInfo, GridHadoopTaskState.FAILED, e, false);
            }
        }
    }

    /**
     * Checks if message was received from parent node and prints warning if not.
     *
     * @param desc Sender process ID.
     * @param msg Received message.
     * @return {@code True} if received from parent node.
     */
    private boolean validateNodeMessage(GridHadoopProcessDescriptor desc, GridHadoopMessage msg) {
        if (!nodeDesc.processId().equals(desc.processId())) {
            log.warning("Received process control request from unknown process (will ignore) [desc=" + desc +
                ", msg=" + msg + ']');

            return false;
        }

        return true;
    }

    /**
     * Stops execution of this process.
     */
    private void terminate() {
        System.exit(1);
    }

    /**
     * Message listener.
     */
    private class MessageListener implements GridHadoopMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridHadoopProcessDescriptor desc, GridHadoopMessage msg) {
            if (msg instanceof GridHadoopTaskExecutionRequest) {
                if (validateNodeMessage(desc, msg))
                    runTasks((GridHadoopTaskExecutionRequest)msg);
            }
            else if (msg instanceof GridHadoopJobInfoUpdateRequest) {
                if (validateNodeMessage(desc, msg))
                    updateTasks((GridHadoopJobInfoUpdateRequest)msg);
            }
            else if (msg instanceof GridHadoopPrepareForJobRequest) {
                if (validateNodeMessage(desc, msg))
                    prepareProcess((GridHadoopPrepareForJobRequest)msg);
            }
            else if (msg instanceof GridHadoopShuffleMessage) {
                try {
                    if (log.isTraceEnabled())
                        log.trace("Received shuffle message [desc=" + desc + ", msg=" + msg + ']');

                    GridHadoopShuffleMessage m = (GridHadoopShuffleMessage)msg;

                    shuffleJob.onShuffleMessage(m);

                    comm.sendMessage(desc, new GridHadoopShuffleAck(m.id(), m.jobId()));
                }
                catch (GridException e) {
                    U.error(log, "Failed to process hadoop shuffle message [desc=" + desc + ", msg=" + msg + ']', e);
                }
            }
            else if (msg instanceof GridHadoopShuffleAck) {
                if (log.isTraceEnabled())
                    log.trace("Received shuffle ack [desc=" + desc + ", msg=" + msg + ']');

                shuffleJob.onShuffleAck((GridHadoopShuffleAck)msg);
            }
            else
                log.warning("Unknown message received (will ignore) [desc=" + desc + ", msg=" + msg + ']');
        }

        /** {@inheritDoc} */
        @Override public void onConnectionLost(GridHadoopProcessDescriptor desc) {
            if (log.isDebugEnabled())
                log.debug("Lost connection with remote process: " + desc);

            if (desc.processId().equals(nodeDesc.processId())) {
                log.warning("Child process lost connection with parent node (will terminate child process).");

                shutdown();

                terminate();
            }
        }
    }
}
