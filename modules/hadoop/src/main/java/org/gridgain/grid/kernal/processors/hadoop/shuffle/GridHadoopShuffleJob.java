/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.counter.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.collections.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.hadoop.GridHadoopJobProperty.*;
import static org.gridgain.grid.util.offheap.unsafe.GridUnsafeMemory.*;

/**
 * Shuffle job.
 */
public class GridHadoopShuffleJob<T> implements AutoCloseable {
    /** */
    private static final int MSG_BUF_SIZE = 128 * 1024;

    /** */
    private final GridHadoopJob job;

    /** */
    private final GridUnsafeMemory mem;

    /** */
    private final boolean needPartitioner;

    /** Collection of task contexts for each reduce task. */
    private final Map<Integer, GridHadoopTaskContext> reducersCtx = new HashMap<>();

    /** Reducers addresses. */
    private T[] reduceAddrs;

    /** Local reducers address. */
    private final T locReduceAddr;

    /** */
    private final GridHadoopShuffleMessage[] msgs;

    /** */
    private final AtomicReferenceArray<GridHadoopMultimap> maps;

    /** */
    private volatile IgniteInClosure2X<T, GridHadoopShuffleMessage> io;

    /** */
    protected ConcurrentMap<Long, IgniteBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>>> sentMsgs =
        new ConcurrentHashMap<>();

    /** */
    private volatile GridWorker snd;

    /** Latch for remote addresses waiting. */
    private final CountDownLatch ioInitLatch = new CountDownLatch(1);

    /** Finished flag. Set on flush or close. */
    private volatile boolean flushed;

    /** */
    private final IgniteLogger log;

    /**
     * @param locReduceAddr Local reducer address.
     * @param log Logger.
     * @param job Job.
     * @param mem Memory.
     * @param totalReducerCnt Amount of reducers in the Job.
     * @param locReducers Reducers will work on current node.
     * @throws GridException If error.
     */
    public GridHadoopShuffleJob(T locReduceAddr, IgniteLogger log, GridHadoopJob job, GridUnsafeMemory mem,
        int totalReducerCnt, int[] locReducers) throws GridException {
        this.locReduceAddr = locReduceAddr;
        this.job = job;
        this.mem = mem;
        this.log = log.getLogger(GridHadoopShuffleJob.class);

        if (!F.isEmpty(locReducers)) {
            for (int rdc : locReducers) {
                GridHadoopTaskInfo taskInfo = new GridHadoopTaskInfo(GridHadoopTaskType.REDUCE, job.id(), rdc, 0, null);

                reducersCtx.put(rdc, job.getTaskContext(taskInfo));
            }
        }

        needPartitioner = totalReducerCnt > 1;

        maps = new AtomicReferenceArray<>(totalReducerCnt);
        msgs = new GridHadoopShuffleMessage[totalReducerCnt];
    }

    /**
     * @param reduceAddrs Addresses of reducers.
     * @return {@code True} if addresses were initialized by this call.
     */
    public boolean initializeReduceAddresses(T[] reduceAddrs) {
        if (this.reduceAddrs == null) {
            this.reduceAddrs = reduceAddrs;

            return true;
        }

        return false;
    }

    /**
     * @return {@code True} if reducers addresses were initialized.
     */
    public boolean reducersInitialized() {
        return reduceAddrs != null;
    }

    /**
     * @param gridName Grid name.
     * @param io IO Closure for sending messages.
     */
    @SuppressWarnings("BusyWait")
    public void startSending(String gridName, IgniteInClosure2X<T, GridHadoopShuffleMessage> io) {
        assert snd == null;
        assert io != null;

        this.io = io;

        if (!flushed) {
            snd = new GridWorker(gridName, "hadoop-shuffle-" + job.id(), log) {
                @Override protected void body() throws InterruptedException {
                    try {
                        while (!isCancelled()) {
                            Thread.sleep(5);

                            collectUpdatesAndSend(false);
                        }
                    }
                    catch (GridException e) {
                        throw new IllegalStateException(e);
                    }
                }
            };

            new IgniteThread(snd).start();
        }

        ioInitLatch.countDown();
    }

    /**
     * @param maps Maps.
     * @param idx Index.
     * @return Map.
     */
    private GridHadoopMultimap getOrCreateMap(AtomicReferenceArray<GridHadoopMultimap> maps, int idx) {
        GridHadoopMultimap map = maps.get(idx);

        if (map == null) { // Create new map.
            map = get(job.info(), SHUFFLE_REDUCER_NO_SORTING, false) ?
                new GridHadoopConcurrentHashMultimap(job.info(), mem, get(job.info(), PARTITION_HASHMAP_SIZE, 8 * 1024)):
                new GridHadoopSkipList(job.info(), mem);

            if (!maps.compareAndSet(idx, null, map)) {
                map.close();

                return maps.get(idx);
            }
        }

        return map;
    }

    /**
     * @param msg Message.
     * @throws GridException Exception.
     */
    public void onShuffleMessage(GridHadoopShuffleMessage msg) throws GridException {
        assert msg.buffer() != null;
        assert msg.offset() > 0;

        GridHadoopTaskContext taskCtx = reducersCtx.get(msg.reducer());

        GridHadoopPerformanceCounter perfCntr = GridHadoopPerformanceCounter.getCounter(taskCtx.counters(), null);

        perfCntr.onShuffleMessage(msg.reducer(), U.currentTimeMillis());

        GridHadoopMultimap map = getOrCreateMap(maps, msg.reducer());

        // Add data from message to the map.
        try (GridHadoopMultimap.Adder adder = map.startAdding(taskCtx)) {
            final GridUnsafeDataInput dataInput = new GridUnsafeDataInput();
            final UnsafeValue val = new UnsafeValue(msg.buffer());

            msg.visit(new GridHadoopShuffleMessage.Visitor() {
                /** */
                private GridHadoopMultimap.Key key;

                @Override public void onKey(byte[] buf, int off, int len) throws GridException {
                    dataInput.bytes(buf, off, off + len);

                    key = adder.addKey(dataInput, key);
                }

                @Override public void onValue(byte[] buf, int off, int len) {
                    val.off = off;
                    val.size = len;

                    key.add(val);
                }
            });
        }
    }

    /**
     * @param ack Shuffle ack.
     */
    @SuppressWarnings("ConstantConditions")
    public void onShuffleAck(GridHadoopShuffleAck ack) {
        IgniteBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>> tup = sentMsgs.get(ack.id());

        if (tup != null)
            tup.get2().onDone();
        else
            log.warning("Received shuffle ack for not registered shuffle id: " + ack);
    }

    /**
     * Unsafe value.
     */
    private static class UnsafeValue implements GridHadoopMultimap.Value {
        /** */
        private final byte[] buf;

        /** */
        private int off;

        /** */
        private int size;

        /**
         * @param buf Buffer.
         */
        private UnsafeValue(byte[] buf) {
            assert buf != null;

            this.buf = buf;
        }

        /** */
        @Override public int size() {
            return size;
        }

        /** */
        @Override public void copyTo(long ptr) {
            UNSAFE.copyMemory(buf, BYTE_ARR_OFF + off, null, ptr, size);
        }
    }

    /**
     * Sends map updates to remote reducers.
     */
    private void collectUpdatesAndSend(boolean flush) throws GridException {
        for (int i = 0; i < maps.length(); i++) {
            GridHadoopMultimap map = maps.get(i);

            if (map == null || locReduceAddr.equals(reduceAddrs[i]))
                continue; // Skip empty map and local node.

            if (msgs[i] == null)
                msgs[i] = new GridHadoopShuffleMessage(job.id(), i, MSG_BUF_SIZE);

            final int idx = i;

            map.visit(false, new GridHadoopMultimap.Visitor() {
                /** */
                private long keyPtr;

                /** */
                private int keySize;

                /** */
                private boolean keyAdded;

                /** {@inheritDoc} */
                @Override public void onKey(long keyPtr, int keySize) {
                    this.keyPtr = keyPtr;
                    this.keySize = keySize;

                    keyAdded = false;
                }

                private boolean tryAdd(long valPtr, int valSize) {
                    GridHadoopShuffleMessage msg = msgs[idx];

                    if (!keyAdded) { // Add key and value.
                        int size = keySize + valSize;

                        if (!msg.available(size, false))
                            return false;

                        msg.addKey(keyPtr, keySize);
                        msg.addValue(valPtr, valSize);

                        keyAdded = true;

                        return true;
                    }

                    if (!msg.available(valSize, true))
                        return false;

                    msg.addValue(valPtr, valSize);

                    return true;
                }

                /** {@inheritDoc} */
                @Override public void onValue(long valPtr, int valSize) {
                    if (tryAdd(valPtr, valSize))
                        return;

                    send(idx, keySize + valSize);

                    keyAdded = false;

                    if (!tryAdd(valPtr, valSize))
                        throw new IllegalStateException();
                }
            });

            if (flush && msgs[i].offset() != 0)
                send(i, 0);
        }
    }

    /**
     * @param idx Index of message.
     * @param newBufMinSize Min new buffer size.
     */
    private void send(final int idx, int newBufMinSize) {
        final GridFutureAdapterEx<?> fut = new GridFutureAdapterEx<>();

        GridHadoopShuffleMessage msg = msgs[idx];

        final long msgId = msg.id();

        IgniteBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>> old = sentMsgs.putIfAbsent(msgId,
            new IgniteBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>>(msg, fut));

        assert old == null;

        try {
            io.apply(reduceAddrs[idx], msg);
        }
        catch (GridClosureException e) {
            fut.onDone(U.unwrap(e));
        }

        fut.listenAsync(new IgniteInClosure<IgniteFuture<?>>() {
            @Override public void apply(IgniteFuture<?> f) {
                try {
                    f.get();

                    // Clean up the future from map only if there was no exception.
                    // Otherwise flush() should fail.
                    sentMsgs.remove(msgId);
                }
                catch (GridException e) {
                    log.error("Failed to send message.", e);
                }
            }
        });

        msgs[idx] = newBufMinSize == 0 ? null : new GridHadoopShuffleMessage(job.id(), idx,
            Math.max(MSG_BUF_SIZE, newBufMinSize));
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        if (snd != null) {
            snd.cancel();

            try {
                snd.join();
            }
            catch (InterruptedException e) {
                throw new GridInterruptedException(e);
            }
        }

        close(maps);
    }

    /**
     * @param maps Maps.
     */
    private void close(AtomicReferenceArray<GridHadoopMultimap> maps) {
        for (int i = 0; i < maps.length(); i++) {
            GridHadoopMultimap map = maps.get(i);

            if (map != null)
                map.close();
        }
    }

    /**
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteFuture<?> flush() throws GridException {
        if (log.isDebugEnabled())
            log.debug("Flushing job " + job.id() + " on address " + locReduceAddr);

        flushed = true;

        if (maps.length() == 0)
            return new GridFinishedFutureEx<>();

        U.await(ioInitLatch);

        GridWorker snd0 = snd;

        if (snd0 != null) {
            if (log.isDebugEnabled())
                log.debug("Cancelling sender thread.");

            snd0.cancel();

            try {
                snd0.join();

                if (log.isDebugEnabled())
                    log.debug("Finished waiting for sending thread to complete on shuffle job flush: " + job.id());
            }
            catch (InterruptedException e) {
                throw new GridInterruptedException(e);
            }
        }

        collectUpdatesAndSend(true); // With flush.

        if (log.isDebugEnabled())
            log.debug("Finished sending collected updates to remote reducers: " + job.id());

        GridCompoundFuture fut = new GridCompoundFuture<>();

        for (IgniteBiTuple<GridHadoopShuffleMessage, GridFutureAdapterEx<?>> tup : sentMsgs.values())
            fut.add(tup.get2());

        fut.markInitialized();

        if (log.isDebugEnabled())
            log.debug("Collected futures to compound futures for flush: " + sentMsgs.size());

        return fut;
    }

    /**
     * @param taskCtx Task context.
     * @return Output.
     * @throws GridException If failed.
     */
    public GridHadoopTaskOutput output(GridHadoopTaskContext taskCtx) throws GridException {
        switch (taskCtx.taskInfo().type()) {
            case MAP:
                assert !job.info().hasCombiner() : "The output creation is allowed if combiner has not been defined.";

            case COMBINE:
                return new PartitionedOutput(taskCtx);

            default:
                throw new IllegalStateException("Illegal type: " + taskCtx.taskInfo().type());
        }
    }

    /**
     * @param taskCtx Task context.
     * @return Input.
     * @throws GridException If failed.
     */
    @SuppressWarnings("unchecked")
    public GridHadoopTaskInput input(GridHadoopTaskContext taskCtx) throws GridException {
        switch (taskCtx.taskInfo().type()) {
            case REDUCE:
                int reducer = taskCtx.taskInfo().taskNumber();

                GridHadoopMultimap m = maps.get(reducer);

                if (m != null)
                    return m.input(taskCtx);

                return new GridHadoopTaskInput() { // Empty input.
                    @Override public boolean next() {
                        return false;
                    }

                    @Override public Object key() {
                        throw new IllegalStateException();
                    }

                    @Override public Iterator<?> values() {
                        throw new IllegalStateException();
                    }

                    @Override public void close() {
                        // No-op.
                    }
                };

            default:
                throw new IllegalStateException("Illegal type: " + taskCtx.taskInfo().type());
        }
    }

    /**
     * Partitioned output.
     */
    private class PartitionedOutput implements GridHadoopTaskOutput {
        /** */
        private final GridHadoopTaskOutput[] adders = new GridHadoopTaskOutput[maps.length()];

        /** */
        private GridHadoopPartitioner partitioner;

        /** */
        private final GridHadoopTaskContext taskCtx;

        /**
         * Constructor.
         * @param taskCtx Task context.
         */
        private PartitionedOutput(GridHadoopTaskContext taskCtx) throws GridException {
            this.taskCtx = taskCtx;

            if (needPartitioner)
                partitioner = taskCtx.partitioner();
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) throws GridException {
            int part = 0;

            if (partitioner != null) {
                part = partitioner.partition(key, val, adders.length);

                if (part < 0 || part >= adders.length)
                    throw new GridException("Invalid partition: " + part);
            }

            GridHadoopTaskOutput out = adders[part];

            if (out == null)
                adders[part] = out = getOrCreateMap(maps, part).startAdding(taskCtx);

            out.write(key, val);
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            for (GridHadoopTaskOutput adder : adders) {
                if (adder != null)
                    adder.close();
            }
        }
    }
}
