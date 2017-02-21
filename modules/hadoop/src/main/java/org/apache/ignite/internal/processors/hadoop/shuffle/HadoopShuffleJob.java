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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopMapperAwareTaskOutput;
import org.apache.ignite.internal.processors.hadoop.HadoopMapperUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopPartitioner;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskOutput;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskType;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopPerformanceCounter;
import org.apache.ignite.internal.processors.hadoop.message.HadoopMessage;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopConcurrentHashMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopSkipList;
import org.apache.ignite.internal.processors.hadoop.shuffle.direct.HadoopDirectDataInput;
import org.apache.ignite.internal.processors.hadoop.shuffle.direct.HadoopDirectDataOutputContext;
import org.apache.ignite.internal.processors.hadoop.shuffle.direct.HadoopDirectDataOutputState;
import org.apache.ignite.internal.util.GridUnsafe;
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
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.zip.GZIPInputStream;

import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.PARTITION_HASHMAP_SIZE;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_JOB_THROTTLE;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_MAPPER_STRIPED_OUTPUT;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_MSG_GZIP;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_MSG_SIZE;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_REDUCER_NO_SORTING;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.get;

/**
 * Shuffle job.
 */
public class HadoopShuffleJob<T> implements AutoCloseable {
    /** */
    private static final int DFLT_SHUFFLE_MSG_SIZE = 1024 * 1024;

    /** */
    private static final boolean DFLT_SHUFFLE_MSG_GZIP = false;

    /** */
    private final HadoopJob job;

    /** */
    private final GridUnsafeMemory mem;

    /** */
    private final boolean needPartitioner;

    /** Task contexts for each reduce task. */
    private final AtomicReferenceArray<LocalTaskContextProxy> locReducersCtx;

    /** Reducers addresses. */
    private T[] reduceAddrs;

    /** Total reducer count. */
    private final int totalReducerCnt;

    /** Local reducers address. */
    private final T locReduceAddr;

    /** */
    private final HadoopShuffleMessage[] msgs;

    /** Maps for local reducers. */
    private final AtomicReferenceArray<HadoopMultimap> locMaps;

    /** Maps for remote reducers. */
    private final AtomicReferenceArray<HadoopMultimap> rmtMaps;

    /** */
    private volatile IgniteInClosure2X<T, HadoopMessage> io;

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

    /** Message size. */
    private final int msgSize;

    /** Whether to GZIP shuffle messages. */
    private final boolean msgGzip;

    /** Whether to strip mappers for remote execution. */
    private final boolean stripeMappers;

    /** Local shuffle states. */
    private volatile HashMap<T, HadoopShuffleLocalState> locShuffleStates = new HashMap<>();

    /** Remote shuffle states. */
    private volatile HashMap<T, HadoopShuffleRemoteState> rmtShuffleStates = new HashMap<>();

    /** Mutex for internal synchronization. */
    private final Object mux = new Object();

    /** */
    private final long throttle;

    /** Embedded mode flag. */
    private final boolean embedded;

    /**
     * @param locReduceAddr Local reducer address.
     * @param log Logger.
     * @param job Job.
     * @param mem Memory.
     * @param totalReducerCnt Amount of reducers in the Job.
     * @param locReducers Reducers will work on current node.
     * @param locMappersCnt Number of mappers running on the given node.
     * @param embedded Whether shuffle is running in embedded mode.
     * @throws IgniteCheckedException If error.
     */
    public HadoopShuffleJob(T locReduceAddr, IgniteLogger log, HadoopJob job, GridUnsafeMemory mem,
        int totalReducerCnt, int[] locReducers, int locMappersCnt, boolean embedded) throws IgniteCheckedException {
        this.locReduceAddr = locReduceAddr;
        this.totalReducerCnt = totalReducerCnt;
        this.job = job;
        this.mem = mem;
        this.log = log.getLogger(HadoopShuffleJob.class);
        this.embedded = embedded;

        // No stripes for combiner.
        boolean stripeMappers0 = get(job.info(), SHUFFLE_MAPPER_STRIPED_OUTPUT, false);

        if (stripeMappers0) {
            if (!embedded) {
                log.info("Striped mapper output is disabled becuase it cannot be used in external mode [jobId=" +
                    job.id() + ']');

                stripeMappers0 = false;
            }
        }

        stripeMappers = stripeMappers0;

        msgSize = get(job.info(), SHUFFLE_MSG_SIZE, DFLT_SHUFFLE_MSG_SIZE);
        msgGzip = get(job.info(), SHUFFLE_MSG_GZIP, DFLT_SHUFFLE_MSG_GZIP);

        locReducersCtx = new AtomicReferenceArray<>(totalReducerCnt);

        if (!F.isEmpty(locReducers)) {
            for (int rdc : locReducers) {
                HadoopTaskInfo taskInfo = new HadoopTaskInfo(HadoopTaskType.REDUCE, job.id(), rdc, 0, null);

                locReducersCtx.set(rdc, new LocalTaskContextProxy(taskInfo));
            }
        }

        needPartitioner = totalReducerCnt > 1;

        // Size of local map is always equal to total reducer number to allow index-based lookup.
        locMaps = new AtomicReferenceArray<>(totalReducerCnt);

        // Size of remote map:
        // - If there are no local mappers, then we will not send anything, so set to 0;
        // - If output is not striped, then match it to total reducer count, the same way as for local maps.
        // - If output is striped, then multiply previous value by number of local mappers.
        int rmtMapsSize = locMappersCnt == 0 ? 0 : totalReducerCnt;

        if (stripeMappers)
            rmtMapsSize *= locMappersCnt;

        rmtMaps = new AtomicReferenceArray<>(rmtMapsSize);
        msgs = new HadoopShuffleMessage[rmtMapsSize];

        throttle = get(job.info(), SHUFFLE_JOB_THROTTLE, 0);
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
    public void startSending(String gridName, IgniteInClosure2X<T, HadoopMessage> io) {
        assert snd == null;
        assert io != null;

        this.io = io;

        if (!stripeMappers) {
            if (!flushed) {
                snd = new GridWorker(gridName, "hadoop-shuffle-" + job.id(), log) {
                    @Override protected void body() throws InterruptedException {
                        try {
                            while (!isCancelled()) {
                                if (throttle > 0)
                                    Thread.sleep(throttle);

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
     * @param src Source.
     * @param msg Message.
     * @throws IgniteCheckedException Exception.
     */
    public void onShuffleMessage(T src, HadoopShuffleMessage msg) throws IgniteCheckedException {
        assert msg.buffer() != null;
        assert msg.offset() > 0;

        HadoopTaskContext taskCtx = locReducersCtx.get(msg.reducer()).get();

        HadoopPerformanceCounter perfCntr = HadoopPerformanceCounter.getCounter(taskCtx.counters(), null);

        perfCntr.onShuffleMessage(msg.reducer(), U.currentTimeMillis());

        HadoopMultimap map = getOrCreateMap(locMaps, msg.reducer());

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

        if (embedded) {
            // No immediate response.
            if (localShuffleState(src).onShuffleMessage())
                sendFinishResponse(src, msg.jobId());
        }
        else
            // Response for every message.
            io.apply(src, new HadoopShuffleAck(msg.id(), msg.jobId()));
    }

    /**
     * Process shuffle message.
     *
     * @param src Source.
     * @param msg Message.
     * @throws IgniteCheckedException Exception.
     */
    public void onDirectShuffleMessage(T src, HadoopDirectShuffleMessage msg) throws IgniteCheckedException {
        byte[] buf = extractBuffer(msg);

        assert buf != null;

        int rdc = msg.reducer();

        HadoopTaskContext taskCtx = locReducersCtx.get(rdc).get();

        HadoopPerformanceCounter perfCntr = HadoopPerformanceCounter.getCounter(taskCtx.counters(), null);

        perfCntr.onShuffleMessage(rdc, U.currentTimeMillis());

        HadoopMultimap map = getOrCreateMap(locMaps, rdc);

        HadoopSerialization keySer = taskCtx.keySerialization();
        HadoopSerialization valSer = taskCtx.valueSerialization();

        // Add data from message to the map.
        try (HadoopMultimap.Adder adder = map.startAdding(taskCtx)) {
            HadoopDirectDataInput in = new HadoopDirectDataInput(buf);

            Object key = null;
            Object val = null;

            for (int i = 0; i < msg.count(); i++) {
                key = keySer.read(in, key);
                val = valSer.read(in, val);

                adder.write(key, val);
            }
        }

        if (localShuffleState(src).onShuffleMessage())
            sendFinishResponse(src, msg.jobId());
    }

    /**
     * Extract buffer from direct shuffle message.
     *
     * @param msg Message.
     * @return Buffer.
     */
    private byte[] extractBuffer(HadoopDirectShuffleMessage msg) throws IgniteCheckedException {
        if (msgGzip) {
            byte[] res = new byte[msg.dataLength()];

            try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(msg.buffer()), res.length)) {
                int len = in.read(res, 0, res.length);

                assert len == res.length;
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to uncompress direct shuffle message.", e);
            }

            return res;
        }
        else
            return msg.buffer();
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
     * Process shuffle finish request.
     *
     * @param src Source.
     * @param msg Shuffle finish message.
     */
    public void onShuffleFinishRequest(T src, HadoopShuffleFinishRequest msg) {
        if (log.isDebugEnabled())
            log.debug("Received shuffle finish request [jobId=" + job.id() + ", src=" + src + ", req=" + msg + ']');

        HadoopShuffleLocalState state = localShuffleState(src);

        if (state.onShuffleFinishMessage(msg.messageCount()))
            sendFinishResponse(src, msg.jobId());
    }

    /**
     * Process shuffle finish response.
     *
     * @param src Source.
     */
    public void onShuffleFinishResponse(T src) {
        if (log.isDebugEnabled())
            log.debug("Received shuffle finish response [jobId=" + job.id() + ", src=" + src + ']');

        remoteShuffleState(src).onShuffleFinishResponse();
    }

    /**
     * Send finish response.
     *
     * @param dest Destination.
     * @param jobId Job ID.
     */
    @SuppressWarnings("unchecked")
    private void sendFinishResponse(T dest, HadoopJobId jobId) {
        if (log.isDebugEnabled())
            log.debug("Sent shuffle finish response [jobId=" + jobId + ", dest=" + dest + ']');

        HadoopShuffleFinishResponse msg = new HadoopShuffleFinishResponse(jobId);

        io.apply(dest, msg);
    }

    /**
     * Get local shuffle state for node.
     *
     * @param src Source
     * @return Local shuffle state.
     */
    private HadoopShuffleLocalState localShuffleState(T src) {
        HashMap<T, HadoopShuffleLocalState> states = locShuffleStates;

        HadoopShuffleLocalState res = states.get(src);

        if (res == null) {
            synchronized (mux) {
                res = locShuffleStates.get(src);

                if (res == null) {
                    res = new HadoopShuffleLocalState();

                    states = new HashMap<>(locShuffleStates);

                    states.put(src, res);

                    locShuffleStates = states;
                }
            }
        }

        return res;
    }

    /**
     * Get remote shuffle state for node.
     *
     * @param src Source.
     * @return Remote shuffle state.
     */
    private HadoopShuffleRemoteState remoteShuffleState(T src) {
        HashMap<T, HadoopShuffleRemoteState> states = rmtShuffleStates;

        HadoopShuffleRemoteState res = states.get(src);

        if (res == null) {
            synchronized (mux) {
                res = rmtShuffleStates.get(src);

                if (res == null) {
                    res = new HadoopShuffleRemoteState();

                    states = new HashMap<>(rmtShuffleStates);

                    states.put(src, res);

                    rmtShuffleStates = states;
                }
            }
        }

        return res;
    }

    /**
     * Get all remote shuffle states.
     *
     * @return Remote shuffle states.
     */
    private HashMap<T, HadoopShuffleRemoteState> remoteShuffleStates() {
        synchronized (mux) {
            return new HashMap<>(rmtShuffleStates);
        }
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
            GridUnsafe.copyHeapOffheap(buf, GridUnsafe.BYTE_ARR_OFF + off, ptr, size);
        }
    }

    /**
     * Send updates to remote reducers.
     *
     * @param flush Flush flag.
     * @throws IgniteCheckedException If failed.
     */
    private void collectUpdatesAndSend(boolean flush) throws IgniteCheckedException {
        for (int i = 0; i < rmtMaps.length(); i++)
            collectUpdatesAndSend(i, flush);
    }

    /**
     * Send updates to concrete remote reducer.
     *
     * @param rmtMapIdx Remote map index.
     * @param flush Flush flag.
     * @throws IgniteCheckedException If failed.
     */
    private void collectUpdatesAndSend(int rmtMapIdx, boolean flush) throws IgniteCheckedException {
        final int rmtRdcIdx = stripeMappers ? rmtMapIdx % totalReducerCnt : rmtMapIdx;

        HadoopMultimap map = rmtMaps.get(rmtMapIdx);

        if (map == null)
            return;

        if (msgs[rmtMapIdx] == null)
            msgs[rmtMapIdx] = new HadoopShuffleMessage(job.id(), rmtRdcIdx, msgSize);

        visit(map, rmtMapIdx, rmtRdcIdx);

        if (flush && msgs[rmtMapIdx].offset() != 0)
            send(rmtMapIdx, rmtRdcIdx, 0);
    }

    /**
     * Flush remote direct context.
     *
     * @param rmtMapIdx Remote map index.
     * @param rmtDirectCtx Remote direct context.
     * @param reset Whether to perform reset.
     */
    private void sendShuffleMessage(int rmtMapIdx, @Nullable HadoopDirectDataOutputContext rmtDirectCtx,
        boolean reset) {
        if (rmtDirectCtx == null)
            return;

        int cnt = rmtDirectCtx.count();

        if (cnt == 0)
            return;

        int rmtRdcIdx = stripeMappers ? rmtMapIdx % totalReducerCnt : rmtMapIdx;

        HadoopDirectDataOutputState state = rmtDirectCtx.state();

        if (reset)
            rmtDirectCtx.reset();

        HadoopDirectShuffleMessage msg = new HadoopDirectShuffleMessage(job.id(), rmtRdcIdx, cnt,
            state.buffer(), state.bufferLength(), state.dataLength());

        T nodeId = reduceAddrs[rmtRdcIdx];

        io.apply(nodeId, msg);

        remoteShuffleState(nodeId).onShuffleMessage();
    }

    /**
     * Visit output map.
     *
     * @param map Map.
     * @param rmtMapIdx Remote map index.
     * @param rmtRdcIdx Remote reducer index.
     * @throws IgniteCheckedException If failed.
     */
    private void visit(HadoopMultimap map, final int rmtMapIdx, final int rmtRdcIdx) throws IgniteCheckedException {
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
                HadoopShuffleMessage msg = msgs[rmtMapIdx];

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

                send(rmtMapIdx, rmtRdcIdx, keySize + valSize);

                keyAdded = false;

                if (!tryAdd(valPtr, valSize))
                    throw new IllegalStateException();
            }
        });
    }

    /**
     * Send message.
     *
     * @param rmtMapIdx Remote map index.
     * @param rmtRdcIdx Remote reducer index.
     * @param newBufMinSize Min new buffer size.
     */
    private void send(int rmtMapIdx, int rmtRdcIdx, int newBufMinSize) {
        HadoopShuffleMessage msg = msgs[rmtMapIdx];

        final long msgId = msg.id();

        final GridFutureAdapter<?> fut;

        if (embedded)
            fut = null;
        else {
            fut = new GridFutureAdapter<>();

            IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>> old = sentMsgs.putIfAbsent(msgId,
                new IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>>(msg, fut));

            assert old == null;
        }

        try {
            io.apply(reduceAddrs[rmtRdcIdx], msg);

            if (embedded)
                remoteShuffleState(reduceAddrs[rmtRdcIdx]).onShuffleMessage();
        }
        catch (GridClosureException e) {
            if (fut != null)
                fut.onDone(U.unwrap(e));
        }

        if (fut != null) {
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
        }

        msgs[rmtMapIdx] = newBufMinSize == 0 ? null : new HadoopShuffleMessage(job.id(), rmtRdcIdx,
            Math.max(msgSize, newBufMinSize));
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

        close(locMaps);
        close(rmtMaps);
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

        if (totalReducerCnt == 0)
            return new GridFinishedFuture<>();

        if (!stripeMappers) {
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
        }

        GridCompoundFuture fut = new GridCompoundFuture<>();

        if (embedded) {
            boolean sent = false;

            for (Map.Entry<T, HadoopShuffleRemoteState> rmtStateEntry : remoteShuffleStates().entrySet()) {
                T dest = rmtStateEntry.getKey();
                HadoopShuffleRemoteState rmtState = rmtStateEntry.getValue();

                HadoopShuffleFinishRequest req = new HadoopShuffleFinishRequest(job.id(), rmtState.messageCount());

                io.apply(dest, req);

                if (log.isDebugEnabled())
                    log.debug("Sent shuffle finish request [jobId=" + job.id() + ", dest=" + dest +
                        ", req=" + req + ']');

                fut.add(rmtState.future());

                sent = true;
            }

            if (sent)
                fut.markInitialized();
            else
                return new GridFinishedFuture<>();
        }
        else {
            for (IgniteBiTuple<HadoopShuffleMessage, GridFutureAdapter<?>> tup : sentMsgs.values())
                fut.add(tup.get2());

            fut.markInitialized();

            if (log.isDebugEnabled())
                log.debug("Collected futures to compound futures for flush: " + sentMsgs.size());
        }

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

                HadoopMultimap m = locMaps.get(reducer);

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
     * Check if certain partition (reducer) is local.
     *
     * @param part Partition.
     * @return {@code True} if local.
     */
    private boolean isLocalPartition(int part) {
        return locReducersCtx.get(part) != null;
    }

    /**
     * Partitioned output.
     */
    public class PartitionedOutput implements HadoopMapperAwareTaskOutput {
        /** */
        private final HadoopTaskOutput[] locAdders = new HadoopTaskOutput[locMaps.length()];

        /** */
        private final HadoopTaskOutput[] rmtAdders = new HadoopTaskOutput[rmtMaps.length()];

        /** Remote direct contexts. */
        private final HadoopDirectDataOutputContext[] rmtDirectCtxs =
            new HadoopDirectDataOutputContext[rmtMaps.length()];

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
                part = partitioner.partition(key, val, totalReducerCnt);

                if (part < 0 || part >= totalReducerCnt)
                    throw new IgniteCheckedException("Invalid partition: " + part);
            }

            HadoopTaskOutput out;

            if (isLocalPartition(part)) {
                out = locAdders[part];

                if (out == null)
                    locAdders[part] = out = getOrCreateMap(locMaps, part).startAdding(taskCtx);
            }
            else {
                if (stripeMappers) {
                    int mapperIdx = HadoopMapperUtils.mapperIndex();

                    assert mapperIdx >= 0;

                    int idx = totalReducerCnt * mapperIdx + part;

                    HadoopDirectDataOutputContext rmtDirectCtx = rmtDirectCtxs[idx];

                    if (rmtDirectCtx == null) {
                        rmtDirectCtx = new HadoopDirectDataOutputContext(msgSize, msgGzip, taskCtx);

                        rmtDirectCtxs[idx] = rmtDirectCtx;
                    }

                    if (rmtDirectCtx.write(key, val))
                        sendShuffleMessage(idx, rmtDirectCtx, true);

                    return;
                }
                else {
                    out = rmtAdders[part];

                    if (out == null)
                        rmtAdders[part] = out = getOrCreateMap(rmtMaps, part).startAdding(taskCtx);
                }
            }

            out.write(key, val);
        }

        /** {@inheritDoc} */
        @Override public void onMapperFinished() throws IgniteCheckedException {
            if (stripeMappers) {
                int mapperIdx = HadoopMapperUtils.mapperIndex();

                assert mapperIdx >= 0;

                for (int i = 0; i < totalReducerCnt; i++) {
                    int idx = totalReducerCnt * mapperIdx + i;

                    sendShuffleMessage(idx, rmtDirectCtxs[idx], false);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            for (HadoopTaskOutput adder : locAdders) {
                if (adder != null)
                    adder.close();
            }

            for (HadoopTaskOutput adder : rmtAdders) {
                if (adder != null)
                    adder.close();
            }
        }
    }

    /**
     * Local task context proxy with delayed initialization.
     */
    private class LocalTaskContextProxy {
        /** Mutex for synchronization. */
        private final Object mux = new Object();

        /** Task info. */
        private final HadoopTaskInfo taskInfo;

        /** Task context. */
        private volatile HadoopTaskContext ctx;

        /**
         * Constructor.
         *
         * @param taskInfo Task info.
         */
        public LocalTaskContextProxy(HadoopTaskInfo taskInfo) {
            this.taskInfo = taskInfo;
        }

        /**
         * Get task context.
         *
         * @return Task context.
         * @throws IgniteCheckedException If failed.
         */
        public HadoopTaskContext get() throws IgniteCheckedException {
            HadoopTaskContext ctx0 = ctx;

            if (ctx0 == null) {
                synchronized (mux) {
                    ctx0 = ctx;

                    if (ctx0 == null) {
                        ctx0 = job.getTaskContext(taskInfo);

                        ctx = ctx0;
                    }
                }
            }

            return ctx0;
        }
    }
}