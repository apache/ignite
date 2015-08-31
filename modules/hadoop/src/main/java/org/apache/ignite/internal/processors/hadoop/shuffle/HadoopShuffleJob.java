/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.shuffle;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopPartitioner;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskType;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopPerformanceCounter;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopConcurrentHashMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopSkipList;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.PARTITION_HASHMAP_SIZE;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_REDUCER_NO_SORTING;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.get;
import static org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory.UNSAFE;

/**
 * Shuffle job.
 */
public class HadoopShuffleJob<T> implements AutoCloseable {
    /** */
    private static final int MSG_BUF_SIZE = 128 * 1024;

    /** */
    private final HadoopJob job;

    /** */
    private final GridUnsafeMemory mem;

    /** */
    private final boolean needPartitioner;

    /** Collection of task contexts for each reduce task. */
    private final Map<Integer, HadoopTaskContext> reducersCtx = new HashMap<>();

    /** Reducers addresses. */
    private T[] reduceAddrs;

    /** Local reducers address. */
    private final T locReduceAddr;

    /** */
    private final HadoopShuffleMessage[] msgs;

    /** */
    private final AtomicReferenceArray<HadoopMultimap> maps;

    /** */
    private volatile IgniteInClosure2X<T, HadoopShuffleMessage> io;

    /** */
    protected ConcurrentMap<Long, IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>>> sentMsgs =
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
     * @throws IgniteCheckedException If error.
     */
    public HadoopShuffleJob(T locReduceAddr, IgniteLogger log, HadoopJob job, GridUnsafeMemory mem,
        int totalReducerCnt, int[] locReducers) throws IgniteCheckedException {
        this.locReduceAddr = locReduceAddr;
        this.job = job;
        this.mem = mem;
        this.log = log.getLogger(HadoopShuffleJob.class);

        if (!F.isEmpty(locReducers)) {
            for (int rdc : locReducers) {
                HadoopTaskInfo taskInfo = new HadoopTaskInfo(HadoopTaskType.REDUCE, job.id(), rdc, 0, null);

                reducersCtx.put(rdc, job.getTaskContext(taskInfo));
            }
        }

        needPartitioner = totalReducerCnt > 1;

        maps = new AtomicReferenceArray<>(totalReducerCnt);
        msgs = new HadoopShuffleMessage[totalReducerCnt];
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
    public void startSending(String gridName, IgniteInClosure2X<T, HadoopShuffleMessage> io) {
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
                    catch (IgniteCheckedException e) {
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
    private HadoopMultimap getOrCreateMap(AtomicReferenceArray<HadoopMultimap> maps, int idx) {
        HadoopMultimap map = maps.get(idx);

        if (map == null) { // Create new map.
            map = get(job.info(), SHUFFLE_REDUCER_NO_SORTING, false) ?
                new HadoopConcurrentHashMultimap(job.info(), mem, get(job.info(), PARTITION_HASHMAP_SIZE, 8 * 1024)):
                new HadoopSkipList(job.info(), mem);

            if (!maps.compareAndSet(idx, null, map)) {
                map.close();

                return maps.get(idx);
            }
        }

        return map;
    }

    /**
     * @param msg Message.
     * @throws IgniteCheckedException Exception.
     */
    public void onShuffleMessage(HadoopShuffleMessage msg) throws IgniteCheckedException {
        assert msg.buffer() != null;
        assert msg.offset() > 0;

        HadoopTaskContext taskCtx = reducersCtx.get(msg.reducer());

        HadoopPerformanceCounter perfCntr = HadoopPerformanceCounter.getCounter(taskCtx.counters(), null);

        perfCntr.onShuffleMessage(msg.reducer(), U.currentTimeMillis());

        HadoopMultimap map = getOrCreateMap(maps, msg.reducer());

        // Add data from message to the map.
        try (HadoopMultimap.Adder adder = map.startAdding(taskCtx)) {
            final GridUnsafeDataInput dataInput = new GridUnsafeDataInput();
            final UnsafeValue val = new UnsafeValue(msg.buffer());

            msg.visit(new HadoopShuffleMessage.Visitor() {
                /** */
                private HadoopMultimap.Key key;

                @Override public void onKey(byte[] buf, int off, int len) throws IgniteCheckedException {
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
    public void onShuffleAck(HadoopShuffleAck ack) {
        IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>> tup = sentMsgs.get(ack.id());

        if (tup != null)
            tup.get2().onDone();
        else
            log.warning("Received shuffle ack for not registered shuffle id: " + ack);
    }

    /**
     * Unsafe value.
     */
    private static class UnsafeValue implements HadoopMultimap.Value {
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
    private void collectUpdatesAndSend(boolean flush) throws IgniteCheckedException {
        for (int i = 0; i < maps.length(); i++) {
            HadoopMultimap map = maps.get(i);

            if (map == null || locReduceAddr.equals(reduceAddrs[i]))
                continue; // Skip empty map and local node.

            if (msgs[i] == null)
                msgs[i] = new HadoopShuffleMessage(job.id(), i, MSG_BUF_SIZE);

            final int idx = i;

            map.visit(false, new HadoopMultimap.Visitor() {
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
                    HadoopShuffleMessage msg = msgs[idx];

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
        final GridFutureAdapter<?> fut = new GridFutureAdapter<>();

        HadoopShuffleMessage msg = msgs[idx];

        final long msgId = msg.id();

        IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>> old = sentMsgs.putIfAbsent(msgId,
            new IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>>(msg, fut));

        assert old == null;

        try {
            io.apply(reduceAddrs[idx], msg);
        }
        catch (GridClosureException e) {
            fut.onDone(U.unwrap(e));
        }

        fut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> f) {
                try {
                    f.get();

                    // Clean up the future from map only if there was no exception.
                    // Otherwise flush() should fail.
                    sentMsgs.remove(msgId);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to send message.", e);
                }
            }
        });

        msgs[idx] = newBufMinSize == 0 ? null : new HadoopShuffleMessage(job.id(), idx,
            Math.max(MSG_BUF_SIZE, newBufMinSize));
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        if (snd != null) {
            snd.cancel();

            try {
                snd.join();
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException(e);
            }
        }

        close(maps);
    }

    /**
     * @param maps Maps.
     */
    private void close(AtomicReferenceArray<HadoopMultimap> maps) {
        for (int i = 0; i < maps.length(); i++) {
            HadoopMultimap map = maps.get(i);

            if (map != null)
                map.close();
        }
    }

    /**
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> flush() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Flushing job " + job.id() + " on address " + locReduceAddr);

        flushed = true;

        if (maps.length() == 0)
            return new GridFinishedFuture<>();

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
                throw new IgniteInterruptedCheckedException(e);
            }
        }

        collectUpdatesAndSend(true); // With flush.

        if (log.isDebugEnabled())
            log.debug("Finished sending collected updates to remote reducers: " + job.id());

        GridCompoundFuture fut = new GridCompoundFuture<>();

        for (IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>> tup : sentMsgs.values())
            fut.add(tup.get2());

        fut.markInitialized();

        if (log.isDebugEnabled())
            log.debug("Collected futures to compound futures for flush: " + sentMsgs.size());

        return fut;
    }

    /**
     * @param taskCtx Task context.
     * @return Output.
     * @throws IgniteCheckedException If failed.
     */
    public HadoopTaskOutput output(HadoopTaskContext taskCtx) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public HadoopTaskInput input(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        switch (taskCtx.taskInfo().type()) {
            case REDUCE:
                int reducer = taskCtx.taskInfo().taskNumber();

                HadoopMultimap m = maps.get(reducer);

                if (m != null)
                    return m.input(taskCtx);

                return new HadoopTaskInput() { // Empty input.
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
    private class PartitionedOutput implements HadoopTaskOutput {
        /** */
        private final HadoopTaskOutput[] adders = new HadoopTaskOutput[maps.length()];

        /** */
        private HadoopPartitioner partitioner;

        /** */
        private final HadoopTaskContext taskCtx;

        /**
         * Constructor.
         * @param taskCtx Task context.
         */
        private PartitionedOutput(HadoopTaskContext taskCtx) throws IgniteCheckedException {
            this.taskCtx = taskCtx;

            if (needPartitioner)
                partitioner = taskCtx.partitioner();
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) throws IgniteCheckedException {
            int part = 0;

            if (partitioner != null) {
                part = partitioner.partition(key, val, adders.length);

                if (part < 0 || part >= adders.length)
                    throw new IgniteCheckedException("Invalid partition: " + part);
            }

            HadoopTaskOutput out = adders[part];

            if (out == null)
                adders[part] = out = getOrCreateMap(maps, part).startAdding(taskCtx);

            out.write(key, val);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            for (HadoopTaskOutput adder : adders) {
                if (adder != null)
                    adder.close();
            }
        }
    }
}