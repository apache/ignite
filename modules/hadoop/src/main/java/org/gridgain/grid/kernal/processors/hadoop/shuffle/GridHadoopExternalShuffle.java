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
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.logger.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Hadoop external shuffle.
 */
public class GridHadoopExternalShuffle extends GridHadoopShuffleAdapter<GridHadoopProcessDescriptor> {
    /** Hadoop external communication. */
    private GridHadoopExternalCommunication comm;

    /** Initialized tasks. */
    private Map<GridHadoopJobId, LocalTasks> initializedTasks = new ConcurrentHashMap<>();

    /**
     * @param log Logger to use.
     * @param comm Communication.
     */
    public GridHadoopExternalShuffle(GridLogger log, GridHadoopExternalCommunication comm) {
        super(log);

        this.comm = comm;
    }

    /**
     * Prepares shuffle for external tasks execution.
     *
     * @param job Job instance.
     * @param hasLocMappers Has local mappers flag.
     */
    public void prepareFor(GridHadoopJob job, boolean hasLocMappers) {
        initializedTasks.put(job.id(), new LocalTasks(job, hasLocMappers));
    }

    /** {@inheritDoc} */
    @Override protected void send0(GridHadoopProcessDescriptor dest, Object msg) {
        try {
            comm.sendMessage(dest, (GridHadoopMessage)msg);
        }
        catch (GridException e) {
            // TODO implement.
            e.printStackTrace();
        }
    }

    /** {@inheritDoc} */
    @Override protected GridHadoopShuffleJob<GridHadoopProcessDescriptor> newJob(GridHadoopJobId jobId)
        throws GridException {
        LocalTasks locTasks = initializedTasks.get(jobId);

        assert locTasks != null : "Local tasks should be initialized before shuffle use: " + jobId;

        return new GridHadoopShuffleJob<>(comm.localProcessDescriptor(), log, locTasks.job, mem,
            locTasks.job.reducers(), locTasks.hasLocMappers);
    }

    /** {@inheritDoc} */
    @Override protected void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected String gridName() {
        return "external";
    }

    /**
     * @param jobId Job ID for which addresses were received.
     * @param rdcAddrs Received addresses.
     * @throws GridException Exception if failed to get shuffle job.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void onAddressesReceived(GridHadoopJobId jobId, GridHadoopProcessDescriptor[] rdcAddrs)
        throws GridException {
        GridHadoopShuffleJob<GridHadoopProcessDescriptor> job = job(jobId);

        synchronized (job) {
            if (job.initializeReduceAddresses(rdcAddrs))
                startSending(job);
        }
    }

    private static class LocalTasks {
        /** Job. */
        private GridHadoopJob job;

        /** Has local mappers flag. */
        private boolean hasLocMappers;

        /**
         * @param job Job instance.
         * @param hasLocMappers Has local mappers flag.
         */
        private LocalTasks(GridHadoopJob job, boolean hasLocMappers) {
            this.job = job;
            this.hasLocMappers = hasLocMappers;
        }
    }
}
