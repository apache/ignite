/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.child;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.logger.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Hadoop process base.
 */
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

    /** Job factory. TODO configure? */
    private GridHadoopJobFactory jobFactory = new GridHadoopDefaultJobFactory();

    /**
     * Starts child process runner.
     */
    public void start(
        GridHadoopExternalCommunication comm,
        GridHadoopProcessDescriptor nodeDesc,
        ExecutorService msgExecSvc,
        GridLogger parentLog
    )
        throws GridException {
        this.comm = comm;
        this.nodeDesc = nodeDesc;
        this.msgExecSvc = msgExecSvc;

        comm.setListener(new MessageListener());
        log = parentLog.getLogger(GridHadoopChildProcessRunner.class);

        // TODO other initialization here.

        // At this point node knows that this process has started.
        comm.sendMessage(this.nodeDesc, new GridHadoopProcessStartedReply());
    }

    /**
     * @param req Task execution request.
     */
    private void initializeTasks(GridHadoopTaskExecutionRequest req) {
        try {
            if (initGuard.compareAndSet(false, true)) {
                log.info("Initializing external hadoop task: " + req);

                GridHadoopJob job = jobFactory.createJob(req.jobId(), req.jobInfo());

                Collection<GridHadoopTask> mappers = null;
                Collection<GridHadoopTask> reducers = null;

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

                initializeExecutors(req, job, mappers, reducers);

                if (mappers != null) {
                    for (GridHadoopTask m : mappers)
                        // TODO input and output.
                        mapperExecSvc.submit(new TaskRunnable(new GridHadoopTaskContext(null, job, null, null), m));

                    if (job.hasCombiner()) {
                        GridHadoopTask c = job.createTask(new GridHadoopTaskInfo(
                            comm.localProcessDescriptor().parentNodeId(),
                            GridHadoopTaskType.COMBINE,
                            job.id(),
                            0,
                            0,
                            null));

                        reducerExecSvc.submit(new TaskRunnable(new GridHadoopTaskContext(null, job, null, null), c));
                    }
                }

                if (reducers != null) {
                    for (GridHadoopTask r : reducers)
                        reducerExecSvc.submit(new TaskRunnable(new GridHadoopTaskContext(null, job, null, null), r));
                }
            }
            else
                log.warning("Received duplicate task execution request for the same process (will ignore): " + req);
        }
        catch (GridException e) {
            log.warning("Unexpected exception caught during task initialization. Will shutdown process runner.", e);

            abortExecution();

            // TODO Send reply.
            e.printStackTrace();
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

        int mapPoolSize = Math.min(req.concurrentMappers(), mappers != null ? mappers.size() : 0);
        // If reducers is null, we will have at least one mapper, so will need one slot for combiner.
        int reducePoolSize = Math.min(req.concurrentReducers(), reducers != null ? reducers.size() :
            job.hasCombiner() ? 1 : 0);

        log.info("Initializing pools [mapPoolSize=" + mapPoolSize + ", reducePoolSize=" + reducePoolSize + ']');

        if (mapPoolSize > 0)
            mapperExecSvc = Executors.newFixedThreadPool(mapPoolSize);

        if (reducePoolSize > 0)
            reducerExecSvc = Executors.newFixedThreadPool(reducePoolSize);
    }

    /**
     * Updates external process map so that shuffle can proceed with sending messages to reducers.
     *
     * @param req Update request.
     */
    private void updateTasks(GridHadoopJobInfoUpdateRequest req) {

    }

    private void abortExecution() {
        if (mapperExecSvc != null)
            mapperExecSvc.shutdownNow();

        if (reducerExecSvc != null)
            reducerExecSvc.shutdownNow();
    }

    /**
     * Notifies node about task finish.
     *
     * @param taskInfo Finished task info.
     * @param state Task finish state.
     * @param err Error, if any.
     */
    private void notifyTaskFinished(GridHadoopTaskInfo taskInfo, GridHadoopTaskState state, Throwable err) {

    }

    /**
     * Task runnable.
     */
    private class TaskRunnable implements Runnable {
        /** Task context. */
        private GridHadoopTaskContext taskCtx;

        /** Task to run. */
        private GridHadoopTask task;

        /**
         * @param taskCtx Task context.
         * @param task Tsak.
         */
        private TaskRunnable(GridHadoopTaskContext taskCtx, GridHadoopTask task) {
            this.taskCtx = taskCtx;
            this.task = task;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            GridHadoopTaskState state = GridHadoopTaskState.COMPLETED;
            Throwable err = null;

            try {
                task.run(taskCtx);
            }
            catch (Throwable e) {
                state = GridHadoopTaskState.FAILED;
                err = e;
            }
            finally {
                notifyTaskFinished(task.info(), state, err);
            }
        }
    }

    /**
     * Message listener.
     */
    private class MessageListener implements GridHadoopMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessageReceived(GridHadoopProcessDescriptor desc, GridHadoopMessage msg) {
            // TODO validate control requests are coming from node.
            if (msg instanceof GridHadoopTaskExecutionRequest) {
                initializeTasks((GridHadoopTaskExecutionRequest)msg);
            }
            else if (msg instanceof GridHadoopJobInfoUpdateRequest) {
                updateTasks((GridHadoopJobInfoUpdateRequest)msg);
            }
        }

        /** {@inheritDoc} */
        @Override public void onConnectionLost(GridHadoopProcessDescriptor desc) {
            if (desc.processId().equals(nodeDesc.processId())) {
                log.warning("Child process lost connection with parent node (will terminate child process).");

                // TODO do we need graceful shutdown here?

                System.exit(1);
            }
        }
    }
}
