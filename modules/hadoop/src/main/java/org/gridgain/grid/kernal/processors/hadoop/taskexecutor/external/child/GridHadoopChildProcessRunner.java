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
import org.gridgain.grid.util.typedef.*;

import java.util.*;
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

    /** Mappers executor service. */
    private ExecutorService mapperExecSvc;

    /** Mappers executor service. */
    private ExecutorService reducerExecSvc;

    /** External communication. */
    private GridHadoopExternalCommunication comm;

    /** Logger. */
    private GridLogger log;

    /** Init guard. */
    private AtomicBoolean initGuard = new AtomicBoolean();

    /** Number of pending tasks. */
    private AtomicInteger pendingMappers = new AtomicInteger();

    /** Number of pending reducers. */
    private AtomicInteger pendingReducers = new AtomicInteger();

    /** Local phase. */
    private GridHadoopJobPhase locPhase;

    /** Mappers. */
    private Collection<GridHadoopTask> mappers;

    /** Reducers. */
    private Collection<GridHadoopTask> reducers;

    /** Job instance. */
    private GridHadoopJob job;

    /** Job factory. */
    private GridHadoopJobFactory jobFactory = new GridHadoopDefaultJobFactory();

    /** Embedded shuffle. */
    private GridHadoopExternalShuffle shuffle;

    /**
     * Starts child process runner.
     */
    public void start(GridHadoopExternalCommunication comm, GridHadoopProcessDescriptor nodeDesc,
        ExecutorService msgExecSvc, GridLogger parentLog)
        throws GridException {
        this.comm = comm;
        this.nodeDesc = nodeDesc;
        this.msgExecSvc = msgExecSvc;

        shuffle = new GridHadoopExternalShuffle(parentLog.getLogger(GridHadoopExternalShuffle.class), comm);

        comm.setListener(new MessageListener());
        log = parentLog.getLogger(GridHadoopChildProcessRunner.class);

        // At this point node knows that this process has started.
        comm.sendMessage(this.nodeDesc, new GridHadoopProcessStartedAck());
    }

    /**
     * @param req Task execution request.
     */
    private void initializeTasks(GridHadoopTaskExecutionRequest req) {
        try {
            if (initGuard.compareAndSet(false, true)) {
                log.info("Initializing external hadoop task: " + req);

                job = jobFactory.createJob(req.jobId(), req.jobInfo());

                for (GridHadoopTaskInfo taskInfo : req.tasks()) {
                    GridHadoopTask task = job.createTask(taskInfo);

                    switch (taskInfo.type()) {
                        case MAP: {
                            if (mappers == null)
                                mappers = new ArrayList<>();

                            mappers.add(task);

                            break;
                        }

                        case REDUCE: {
                            if (reducers == null)
                                reducers = new ArrayList<>();

                            reducers.add(task);

                            break;
                        }

                        default:
                            throw new GridException("Invalid task type for external execution: " + taskInfo.type());
                    }
                }

                shuffle.prepareFor(job, mappers != null);

                if (mappers != null)
                    pendingMappers.set(mappers.size());

                if (reducers != null)
                    pendingReducers.set(reducers.size());

                initializeExecutors(req, job, mappers, reducers);

                phase(GridHadoopJobPhase.PHASE_MAP);

                // Can send reply after shuffle has been initialized.
                comm.sendMessage(nodeDesc, new GridHadoopTaskExecutionResponse(job.id(), reducerIds(reducers)));
            }
            else
                log.warning("Received duplicate task execution request for the same process (will ignore): " + req);
        }
        catch (GridException e) {
            log.error("Unexpected exception caught during task initialization (will abort process execution).", e);

            shutdown();

            terminate();
        }
    }

    /**
     * Updates local process state.
     *
     * @param phase Global job state.
     */
    private void phase(GridHadoopJobPhase phase) {
        boolean change = false;
        GridHadoopJobPhase oldPhase = null;

        synchronized (this) {
            if (locPhase != phase) {
                oldPhase = locPhase;

                log.info("Changing process state [old=" + locPhase + ", new=" + phase + ']');

                change = true;

                locPhase = phase;
            }
        }

        if (change) {
            if (phase == GridHadoopJobPhase.PHASE_MAP) {
                if (mappers != null) {
                    for (GridHadoopTask m : mappers)
                        mapperExecSvc.submit(new TaskRunnable(m));

                    log.debug("Submitted tasks to map executor service: " + mappers.size());
                }
            }
            else if (phase == GridHadoopJobPhase.PHASE_REDUCE) {
                if (reducers != null) {
                    for (GridHadoopTask r : reducers)
                        reducerExecSvc.submit(new TaskRunnable(r));

                    log.debug("Submitted tasks to reduce executor service: " + reducers.size());
                }
            }
            else if (phase == GridHadoopJobPhase.PHASE_CANCELLING) {
                if (log.isDebugEnabled())
                    log.debug("Received cancellation request (will terminate all ongoing tasks).");

                if (mapperExecSvc != null) {
                    List<Runnable> incomplete = mapperExecSvc.shutdownNow();

                    for (Runnable task : incomplete) {
                        if (task instanceof TaskRunnable) {
                            TaskRunnable r = (TaskRunnable)task;

                            notifyTaskFinished(r.task.info(), GridHadoopTaskState.CANCELED, null, false);
                        }
                    }
                }

                if (reducerExecSvc != null) {
                    List<Runnable> incomplete = reducerExecSvc.shutdownNow();

                    for (Runnable task : incomplete) {
                        if (task instanceof TaskRunnable) {
                            TaskRunnable r = (TaskRunnable)task;

                            notifyTaskFinished(r.task.info(), GridHadoopTaskState.CANCELED, null, false);
                        }
                    }
                }

                if (oldPhase == GridHadoopJobPhase.PHASE_MAP && reducers != null) {
                    for (GridHadoopTask reduceTask : reducers) {
                        onTaskFinished(reduceTask.info(), GridHadoopTaskState.CANCELED, null);

                    }
                }
            }

            checkFinished();
        }
    }

    /**
     *
     */
    private void checkFinished() {
        if (locPhase == GridHadoopJobPhase.PHASE_REDUCE) {
            if (reducers == null || pendingReducers.get() == 0) {
                if (log.isDebugEnabled())
                    log.debug("Finished running all local tasks, shutting down the process.");

                shutdown();

                terminate();
            }
        }
    }

    /**
     * Creates executor services.
     *
     * @param req Task execution request.
     * @param job Job.
     * @param mappers Collection of mappers (may be null).
     * @param reducers Collection of reducers (may be null).
     */
    private void initializeExecutors(GridHadoopTaskExecutionRequest req,
        GridHadoopJob job,
        Collection<GridHadoopTask> mappers,
        Collection<GridHadoopTask> reducers) {
        assert mappers != null || reducers != null : "Cannot have both mappers and reducers as null";

        int concMappers = Runtime.getRuntime().availableProcessors();
        int concReducers = Runtime.getRuntime().availableProcessors();

        GridHadoopJobInfo info = req.jobInfo();

        if (info instanceof GridHadoopDefaultJobInfo) {
            GridHadoopDefaultJobInfo dfltInfo = (GridHadoopDefaultJobInfo)info;

            JobConf cfg = dfltInfo.configuration();

            concMappers = cfg.getInt(EXTERNAL_CONCURRENT_MAPPERS.propertyName(), concMappers);
            concReducers = cfg.getInt(EXTERNAL_CONCURRENT_REDUCERS.propertyName(), concReducers);
        }

        int mapPoolSize = Math.min(concMappers, mappers != null ? mappers.size() : 0);
        // If reducers is null, we will have at least one mapper, so will need one slot for combiner.
        int reducePoolSize = Math.min(concReducers, reducers != null ? reducers.size() :
            job.hasCombiner() ? 1 : 0);

        log.info("Initializing pools [mapPoolSize=" + mapPoolSize + ", reducePoolSize=" + reducePoolSize + ']');

        if (mapPoolSize > 0)
            mapperExecSvc = Executors.newFixedThreadPool(mapPoolSize);

        if (reducePoolSize > 0)
            reducerExecSvc = Executors.newFixedThreadPool(reducePoolSize);
    }

    /**
     * Creates task output.
     *
     * @param taskInfo Task info.
     * @return Task output.
     */
    private GridHadoopTaskOutput createOutput(GridHadoopTaskInfo taskInfo) throws GridException {
        if (taskInfo.type() == REDUCE || taskInfo.type() == COMMIT || taskInfo.type() == ABORT)
            return null;

        return shuffle.output(taskInfo);
    }

    /**
     * Creates task input.
     *
     * @param taskInfo Task info.
     * @return Task input.
     */
    private GridHadoopTaskInput createInput(GridHadoopTaskInfo taskInfo) throws GridException {
        if (taskInfo.type() == MAP || taskInfo.type() == COMMIT || taskInfo.type() == ABORT)
            return null;

        return shuffle.input(taskInfo);
    }

    /**
     * Updates external process map so that shuffle can proceed with sending messages to reducers.
     *
     * @param req Update request.
     */
    private void updateTasks(GridHadoopJobInfoUpdateRequest req) {
        assert initGuard.get();

        assert req.jobId().equals(job.id());

        try {
            phase(req.jobPhase());

            if (req.reducersAddresses() != null)
                shuffle.onAddressesReceived(req.jobId(), req.reducersAddresses());
        }
        catch (GridException e) {
            log.error("Failed to update tasks state (will terminate process).", e);

            shutdown();

            terminate();
        }
    }

    /**
     * Stops all executors and running tasks.
     */
    private void shutdown() {
        if (mapperExecSvc != null)
            mapperExecSvc.shutdownNow();

        if (reducerExecSvc != null)
            reducerExecSvc.shutdownNow();

        if (msgExecSvc != null)
            msgExecSvc.shutdownNow();
    }

    /**
     * Notifies node about task finish.
     *
     * @param taskInfo Finished task info.
     * @param state Task finish state.
     * @param err Error, if any.
     */
    private void onTaskFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskState state, Throwable err) {
        if (log.isDebugEnabled())
            log.debug("Hadoop task execution finished [taskInfo=" + taskInfo
                + ", state=" + state + ", err=" + err + ']');

        boolean flush = false;

        if (taskInfo.type() == GridHadoopTaskType.MAP) {
            int mappers = pendingMappers.decrementAndGet();

            assert mappers >= 0 : "Invalid pending mappers count: " + mappers;

            // Check if we should start combiner.
            if (mappers == 0) {
                if (log.isDebugEnabled())
                    log.debug("All local mappers have finished execution.");

                if (job.hasCombiner()) {
                    if (log.isDebugEnabled())
                        log.debug("Starting combiner task.");

                    GridHadoopTask c = job.createTask(new GridHadoopTaskInfo(
                        comm.localProcessDescriptor().parentNodeId(),
                        COMBINE,
                        job.id(),
                        0,
                        0,
                        null));

                    reducerExecSvc.submit(new TaskRunnable(c));

                    log.debug("Submitted tasks to reduce executor service: 1");
                }
                else
                    flush = true;
            }
        }
        else if (taskInfo.type() == REDUCE) {
            int rdc = pendingReducers.decrementAndGet();

            assert rdc >= 0;
        }
        else if (taskInfo.type() == COMBINE)
            flush = true;

        notifyTaskFinished(taskInfo, state, err, flush);

        checkFinished();
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

            shuffle.flush(job.id()).listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> f) {
                    if (log.isDebugEnabled())
                        log.debug("Finished flushing shuffle messages [taskInfo=" + taskInfo + "]");

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
     * @param reduceTasks Reduce tasks.
     * @return Reducer IDs.
     */
    private Collection<Integer> reducerIds(Collection<GridHadoopTask> reduceTasks) {
        if (reduceTasks == null)
            return null;

        Collection<Integer> rdc = new ArrayList<>(reduceTasks.size());

        for (GridHadoopTask task : reduceTasks) {
            assert task.info().type() == GridHadoopTaskType.REDUCE;

            rdc.add(task.info().taskNumber());
        }

        return rdc;
    }

    /**
     * Task runnable.
     */
    private class TaskRunnable implements Runnable {
        /** Task to run. */
        private GridHadoopTask task;

        /**
         * @param task Tsak.
         */
        private TaskRunnable(GridHadoopTask task) {
            this.task = task;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            GridHadoopTaskState state = GridHadoopTaskState.COMPLETED;
            Throwable err = null;

            try (GridHadoopTaskOutput out = createOutput(task.info());
                GridHadoopTaskInput in = createInput(task.info())) {

                GridHadoopTaskContext ctx = new GridHadoopTaskContext(null, job, in, out);

                task.run(ctx);
            }
            catch (Throwable e) {
                state = GridHadoopTaskState.FAILED;
                err = e;
            }
            finally {
                onTaskFinished(task.info(), state, err);
            }
        }
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
                    initializeTasks((GridHadoopTaskExecutionRequest)msg);
            }
            else if (msg instanceof GridHadoopJobInfoUpdateRequest) {
                if (validateNodeMessage(desc, msg))
                    updateTasks((GridHadoopJobInfoUpdateRequest)msg);
            }
            else if (msg instanceof GridHadoopShuffleMessage || msg instanceof GridHadoopShuffleAck) {
                if (log.isDebugEnabled())
                    log.debug("Received shuffle message [desc=" + desc + ", msg=" + msg + ']');

                shuffle.onMessageReceived(desc, msg);
            }
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
