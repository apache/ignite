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
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.worker.*;

import java.util.*;
import java.util.concurrent.*;

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
    private final GridHadoopPartitioner partitioner;

    /** */
    private GridHadoopMultimap combinerMap;

    /** */
    private GridHadoopMultimap[] outMaps;

    /** */
    private UUID[] outNodes;

    /** */
    private GridHadoopShuffleMessage[] msgs;

    /** */
    private final ConcurrentMap<Integer, GridHadoopMultimap> inMaps = new ConcurrentHashMap<>();

    /** */
    private final GridHadoopContext ctx;

    /** */
    private GridWorker sender;

    /**
     * @param job Job.
     * @param mem Memory.
     * @param plan Plan.
     */
    public GridHadoopShuffleJob(GridHadoopContext ctx, GridLogger log, GridHadoopJob job, GridUnsafeMemory mem,
        GridHadoopMapReducePlan plan)
        throws GridException {
        this.ctx = ctx;
        this.job = job;
        this.mem = mem;

        partitioner = job.partitioner();

        outMaps = new GridHadoopMultimap[plan.reducers()];

        int outMapSize = get(job, PARTITION_HASHMAP_SIZE, 1024);

        for (int i = 0; i < outMaps.length; i++) {
            UUID nodeId = plan.nodeForReducer(i);

            assert nodeId != null;

            outNodes[i] = nodeId;
            outMaps[i] = new GridHadoopMultimap(job, mem, outMapSize);
        }

        if (job.hasCombiner())
            combinerMap = new GridHadoopMultimap(job, mem, get(job, COMBINER_HASHMAP_SIZE, 1024));

        sender = new GridWorker(ctx.kernalContext().gridName(), "hadoop-shuffle-" + job.id(), log) {
            @Override protected void body() throws InterruptedException, GridInterruptedException {
                while (!isCancelled()) {
                    Thread.sleep(10);

                    collectUpdatesAndSend();
                }
            }
        };

        new GridThread(sender).start();
    }

    private void collectUpdatesAndSend() {
        for (int i = 0; i < outMaps.length; i++) {
            UUID nodeId = outNodes[i];

            outMaps[i].visit(false, new GridHadoopMultimap.Visitor() {
                @Override public void onKey(long keyPtr, int keyLen) {

                }

                @Override public void onValue(long valPtr, int valSize) {

                }
            });


        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        for (GridHadoopMultimap out : outMaps) {
            if (out != null)
                out.close();
        }

        for (GridHadoopMultimap in : inMaps.values()) {
            if (in != null)
                in.close();
        }
    }

    /**
     * @return Future.
     */
    public GridFuture<?> flush() {
        return null; // TODO
    }

    /**
     * @param taskInfo Task info.
     * @return Output.
     * @throws GridException If failed.
     */
    public GridHadoopTaskOutput output(GridHadoopTaskInfo taskInfo) throws GridException {
        switch (taskInfo.type()) {
            case MAP:
                if (job.hasCombiner())
                    return new MapOutput(combinerMap.startAdding());

            case COMBINE:
                return new PartitionedOutput();

            default:
                throw new IllegalStateException("Illegal type: " + taskInfo.type());
        }
    }

    /**
     * @param taskInfo Task info.
     * @return Input.
     * @throws GridException If failed.
     */
    public GridHadoopTaskInput input(GridHadoopTaskInfo taskInfo) throws GridException {
        switch (taskInfo.type()) {
            case COMBINE:
                return combinerMap.input();

            case REDUCE:
                GridHadoopMultimap m = inMaps.get(taskInfo.taskNumber());

                if (m != null)
                    return m.input();

                return new GridHadoopTaskInput() {
                    @Override public boolean next() {
                        return false;
                    }

                    @Override public Object key() {
                        throw new IllegalStateException();
                    }

                    @Override public Iterator<?> values() {
                        throw new IllegalStateException();
                    }

                    @Override public void close() throws Exception {
                        // No-op.
                    }
                };

            default:
                throw new IllegalStateException("Illegal type: " + taskInfo.type());
        }
    }

    /**
     * Map output.
     */
    private class MapOutput implements GridHadoopTaskOutput {
        /** */
        private final GridHadoopMultimap.Adder adder;

        /**
         * @param adder Map adder.
         */
        private MapOutput(GridHadoopMultimap.Adder adder) {
            this.adder = adder;
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) throws GridException {
            adder.add(key, val);
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            adder.close();
        }
    }

    /**
     * Partitioned output.
     */
    private class PartitionedOutput implements GridHadoopTaskOutput {
        /** */
        private GridHadoopMultimap.Adder[] adders;

        /**
         *
         */
        private PartitionedOutput() throws GridException {
            adders = new GridHadoopMultimap.Adder[outMaps.length];

            for (int i = 0; i < adders.length; i++)
                adders[i] = outMaps[i].startAdding();
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) throws GridException {
            int part = partitioner.partition(key, val, adders.length);

            if (part < 0 || part >= adders.length)
                throw new IllegalStateException("Invalid partition: " + part);

            adders[part].add(key, val);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            for (GridHadoopMultimap.Adder adder : adders)
                adder.close();
        }
    }
}
