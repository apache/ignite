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
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.offheap.unsafe.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopJobProperty.*;

/**
 * Shuffle job.
 */
public class GridHadoopShuffleJob implements AutoCloseable {
    /** */
    private final GridHadoopJob job;

    /** */
    private final GridUnsafeMemory mem;

    /** */
    private GridHadoopPartitioner partitioner;

    /** */
    private GridHadoopMultimap combinerMap;

    /** */
    private GridHadoopMultimap[] outs;

    /** */
    private GridHadoopMultimap[] ins;

    /**
     * @param job Job.
     * @param mem Memory.
     * @param reducers Reducers.
     */
    public GridHadoopShuffleJob(GridHadoopJob job, GridUnsafeMemory mem, int reducers) throws GridException {
        this.job = job;
        this.mem = mem;

        partitioner = job.partitioner();

        outs = new GridHadoopMultimap[reducers];
        ins = new GridHadoopMultimap[reducers];
    }

    /** {@inheritDoc} */
    @Override public void close() {
        for (int i = 0; i < outs.length; i++) {
            if (outs[i] != null)
                outs[i].close();
        }

        for (int i = 0; i < ins.length; i++) {
            if (ins[i] != null)
                ins[i].close();
        }
    }

    public synchronized GridHadoopTaskOutput output(GridHadoopTaskInfo taskInfo) throws GridException {
        switch (taskInfo.type()) {
            case MAP:
                if (job.hasCombiner()) {
                    if (combinerMap == null)
                        combinerMap = new GridHadoopMultimap(job, mem, get(job, COMBINER_HASHMAP_SIZE, 1024));

                    return new MapOut(combinerMap.startAdding());
                }

                return null; // TODO

            case COMBINE:
                // TODO

            default:
                throw new IllegalStateException("Illegal type: " + taskInfo.type());
        }
    }

    public GridHadoopTaskInput input(GridHadoopTaskInfo taskInfo) throws GridException {
        switch (taskInfo.type()) {
            case COMBINE:
                return combinerMap.input();

            case REDUCE:
                // TODO

            default:
                throw new IllegalStateException("Illegal type: " + taskInfo.type());
        }
    }

    /**
     * Map output.
     */
    private class MapOut implements GridHadoopTaskOutput {
        /** */
        private final GridHadoopMultimap.Adder adder;

        /**
         * @param adder Map adder.
         */
        private MapOut(GridHadoopMultimap.Adder adder) {
            this.adder = adder;
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) throws GridException {
            adder.add(key, val);
        }

        /** {@inheritDoc} */
        @Override public GridFuture<?> finish() {
            return new GridFinishedFuture<>();
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            adder.close();
        }
    }
}
