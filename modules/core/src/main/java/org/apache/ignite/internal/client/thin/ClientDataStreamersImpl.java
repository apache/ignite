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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteDataStreamerTimeoutException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientDataStreamer;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Thin client data streamers facade implementation.
 */
class ClientDataStreamersImpl {
    /** Receiver thread prefix. */
    static final String THREAD_NAME = "thin-client-streamer";

    /** Channel. */
    private final ReliableChannel ch;

    /** Utils for serialization/deserialization. */
    private final ClientUtils utils;

    /** Executor to async data send. */
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, r -> {
        Thread t = new Thread(r);

        t.setDaemon(true);
        t.setName(THREAD_NAME);

        return t;
    });

    /**
     * @param ch Channel.
     * @param marsh Marshaller.
     */
    ClientDataStreamersImpl(ReliableChannel ch, ClientBinaryMarshaller marsh) {
        this.ch = ch;

        utils = new ClientUtils(marsh);
    }

    /**
     * Create an instance of {@code ClientDataStreamer} for specified cache.
     *
     * @param cacheName Cache name.
     */
    public <K, V> ClientDataStreamer<K, V> create(String cacheName) {
        A.notNull("cacheName", cacheName);

        T2<ClientChannel, Long> streamerParams = ch.service(
            ClientOperation.DATA_STREAMER_CREATE,
            req -> utils.writeObject(req.out(), cacheName),
            res -> new T2<>(res.clientChannel(), res.in().readLong())
        );

        return new ClientDataStreamerImpl<>(cacheName, streamerParams.get1(), streamerParams.get2());
    }

    /**
     * Stop streamers.
     */
    public void stop() {
        executor.shutdownNow();
    }

    /**
     * Implementation of {@link ClientDataStreamer}.
     */
    private class ClientDataStreamerImpl<K, V> implements ClientDataStreamer<K, V> {
        /**
         * Max count of pending requests. If this value is exceeded, thread sending requests will wait for delivery
         * acknowledgement for already sent requests.
         */
        private static final int MAX_PENDING_REQS = 10;

        /**
         * Max count of pending entries. If this value is exceeded, thread adding new entries will wait for sending of
         * already added entries.
         */
        private static final int MAX_PENDING_ENTRIES = 10_000;

        /** Allow overwrite flag mask. */
        private static final byte ALLOW_OVERWRITE_FLAG_MASK = 0x01;

        /** Skip store flag mask. */
        private static final byte SKIP_STORE_FLAG_MASK = 0x02;

        /** Keep binary flag mask. */
        private static final byte KEEP_BINARY_FLAG_MASK = 0x04;

        /** Cache name. */
        private final String cacheName;

        /** Channel. */
        private final ClientChannel ch;

        /** Resource id. */
        private final long rsrcId;

        /** Pending buffers. There are two switching buffers in this array. */
        private final PendingBuffer[] pendingBuf;

        /** Pointer to current buffer. */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private int curBufIdx;

        /** Buffer lock. */
        private final Lock bufLock = new ReentrantLock();

        /** Buffer send lock. */
        private final Lock bufSendLock = new ReentrantLock();

        /** Buffer switch condition. */
        private final Condition bufSwitchCondition = bufLock.newCondition();

        /** Closed flag. */
        private volatile boolean closed;

        /** Mutext to protect pending reqs. */
        private final Object pendingReqsMux = new Object();

        /** Streamer future. */
        private final CompletableFuture<?> streamerFut = new CompletableFuture<>();

        /** Fail counter. */
        private final AtomicInteger failCntr = new AtomicInteger();

        /** Pending reqs count. */
        private int pendingReqsCnt;

        /** Allow overwrite flag. */
        private volatile boolean allowOverwrite;

        /** Skip store flag. */
        private volatile boolean skipStore;

        /** Keep binary flag. */
        private volatile boolean keepBinary;

        /** Buffer size. */
        private volatile int bufSize = DFLT_BUFFER_SIZE;

        /** Timeout. */
        private volatile long timeout = Long.MAX_VALUE;

        /** Auto flush frequency. */
        private volatile long autoFlushFreq;

        /**
         * @param cacheName Cache name.
         * @param ch Channel.
         * @param rsrcId Resource ID.
         */
        ClientDataStreamerImpl(String cacheName, ClientChannel ch, long rsrcId) {
            this.cacheName = cacheName;
            this.ch = ch;
            this.rsrcId = rsrcId;

            pendingBuf = new PendingBuffer[] { new PendingBuffer(utils), new PendingBuffer(utils) };
        }

        /** {@inheritDoc} */
        @Override public String cacheName() {
            return cacheName;
        }

        /** {@inheritDoc} */
        @Override public boolean allowOverwrite() {
            return allowOverwrite;
        }

        /** {@inheritDoc} */
        @Override public void allowOverwrite(boolean allowOverwrite) {
            this.allowOverwrite = allowOverwrite;

            changeFlags();
        }

        /** {@inheritDoc} */
        @Override public boolean skipStore() {
            return skipStore;
        }

        /** {@inheritDoc} */
        @Override public void skipStore(boolean skipStore) {
            this.skipStore = skipStore;

            changeFlags();
        }

        /** {@inheritDoc} */
        @Override public boolean keepBinary() {
            return keepBinary;
        }

        /** {@inheritDoc} */
        @Override public void keepBinary(boolean keepBinary) {
            this.keepBinary = keepBinary;

            changeFlags();
        }

        /**
         * Send change flags request.
         */
        private void changeFlags() {
            try {
                ch.service(
                    ClientOperation.DATA_STREAMER_FLAGS,
                    req -> {
                        req.out().writeLong(rsrcId);
                        req.out().writeByte((byte)((allowOverwrite ? ALLOW_OVERWRITE_FLAG_MASK : 0) |
                                (skipStore ? SKIP_STORE_FLAG_MASK : 0) |
                                (keepBinary ? KEEP_BINARY_FLAG_MASK : 0))
                        );
                    },
                    null
                );
            }
            catch (ClientError e) {
                failCntr.incrementAndGet();

                throw new ClientException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int bufferSize() {
            return bufSize;
        }

        /** {@inheritDoc} */
        @Override public void bufferSize(int bufSize) {
            A.ensure(bufSize > 0, "bufSize > 0");

            this.bufSize = bufSize;
        }

        /** {@inheritDoc} */
        @Override public void timeout(long timeout) {
            A.ensure(timeout > 0L || timeout == -1L, "timeout > 0 || timeout == -1");

            this.timeout = timeout == -1L ? Long.MAX_VALUE : timeout;
        }

        /** {@inheritDoc} */
        @Override public long timeout() {
            return timeout == Long.MAX_VALUE ? -1L : timeout;
        }

        /** {@inheritDoc} */
        @Override public long autoFlushFrequency() {
            return autoFlushFreq;
        }

        /** {@inheritDoc} */
        @Override public void autoFlushFrequency(long autoFlushFreq) {
            A.ensure(autoFlushFreq >= 0, "autoFlushFreq >= 0");

            this.autoFlushFreq = autoFlushFreq;
        }

        /** {@inheritDoc} */
        @Override public IgniteClientFuture<?> future() {
            return new IgniteClientFutureImpl<>(streamerFut);
        }

        /** {@inheritDoc} */
        @Override public IgniteClientFuture<?> removeData(K key)
            throws CacheException, IgniteInterruptedException, IllegalStateException {
            A.notNull(key, "key");

            return addData0(null, key, null);
        }

        /** {@inheritDoc} */
        @Override public IgniteClientFuture<?> addData(Map.Entry<K, V> entry)
            throws CacheException, IgniteInterruptedException, IllegalStateException, IgniteDataStreamerTimeoutException {
            A.notNull(entry, "entry");

            return addData0(null, entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public IgniteClientFuture<?> addData(Map<K, V> entries)
            throws IllegalStateException, IgniteDataStreamerTimeoutException {
            A.notNull(entries, "entries");

            return addData0(entries.entrySet(), null, null);
        }

        /** {@inheritDoc} */
        @Override public IgniteClientFuture<?> addData(K key, @Nullable V val)
            throws CacheException, IgniteInterruptedException, IllegalStateException, IgniteDataStreamerTimeoutException {
            A.notNull(key, "key");

            return addData0(null, key, val);
        }

        /** {@inheritDoc} */
        @Override public IgniteClientFuture<?> addData(Collection<? extends Map.Entry<K, V>> entries)
            throws IllegalStateException, IgniteDataStreamerTimeoutException {
            A.notNull(entries, "entries");

            return addData0(entries, null, null);
        }

        /**
         * Add to pending buffer collection of entries or single entry.
         *
         * @param entries Collection of entries to add or {@code null} to add single entry ({@code key}/{@code val}).
         * @param key Key.
         * @param val Value.
         */
        private IgniteClientFuture<?> addData0(Collection<? extends Map.Entry<K, V>> entries, K key, @Nullable V val) {
            assert (entries == null) ^ (key == null) : "entries=" + entries + ", key=" + key + ", val=" + val;

            long startNanos = System.nanoTime();

            lockWithTimeout(bufLock, timeout);

            try {
                PendingBuffer buf = currentPendingBuffer();

                while (buf.entriesCnt.get() >= MAX_PENDING_ENTRIES && !closed) {
                    try {
                        long timeToWait = timeout - U.millisSinceNanos(startNanos);

                        // Wait if there is too much unsent data.
                        if (timeToWait < 0 || !bufSwitchCondition.await(timeToWait, TimeUnit.MILLISECONDS)) {
                            throw new ClientException(new TimeoutException("Data streamer exceeded timeout " +
                                    "while waiting for pending entries sending."));
                        }

                        buf = currentPendingBuffer();
                    }
                    catch (InterruptedException e) {
                        throw new ClientException(e);
                    }
                }

                checkClosed();

                int totalEntries;

                if (entries != null) {
                    for (Map.Entry<K, V> entry : entries) {
                        buf.writer.writeObject(entry.getKey());
                        buf.writer.writeObject(entry.getValue());
                    }

                    totalEntries = buf.entriesCnt.addAndGet(entries.size());
                }
                else {
                    buf.writer.writeObject(key);
                    buf.writer.writeObject(val);

                    totalEntries = buf.entriesCnt.incrementAndGet();
                }

                long autoFlushFreq = this.autoFlushFreq;

                if (autoFlushFreq > 0 && buf.flushScheduled.compareAndSet(false, true))
                    executor.schedule((Runnable)this::flush0, autoFlushFreq, TimeUnit.MILLISECONDS);

                if (totalEntries >= bufSize && buf.sendScheduled.compareAndSet(false, true))
                    executor.submit((Runnable)this::sendData);

                return buf.pubFut;
            }
            finally {
                bufLock.unlock();
            }
        }

        /** */
        private PendingBuffer currentPendingBuffer() {
            return pendingBuf[curBufIdx];
        }

        /** Send data without timeout. */
        private void sendData() {
            sendData(Long.MAX_VALUE);
        }

        /** */
        private void sendData(long timeout) {
            PendingBuffer buf;

            long startNanos = System.nanoTime();

            lockWithTimeout(bufLock, timeout);

            try {
                if (closed)
                    return;

                buf = currentPendingBuffer();

                if (buf.entriesCnt.get() == 0)
                    return;

                // Acquire send lock before releasing buffer lock. It makes possible to add new data to the new pending
                // buffer, but impossible to switch buffer again, until previous buffer sent to the server.
                lockWithTimeout(bufSendLock, timeout - U.millisSinceNanos(startNanos));

                try {
                    // Switch current buffer.
                    curBufIdx = 1 - curBufIdx;

                    assert currentPendingBuffer().entriesCnt.get() == 0 : currentPendingBuffer().entriesCnt.get();

                    bufSwitchCondition.signalAll();
                }
                catch (Throwable e) {
                    bufSendLock.unlock();

                    throw e;
                }
            }
            finally {
                bufLock.unlock();
            }

            try {
                incrementPendingReqs(timeout - U.millisSinceNanos(startNanos));

                CompletableFuture<Void> fut = buf.intFut;

                ch.serviceAsync(ClientOperation.DATA_STREAMER_ADD,
                    req -> {
                        req.out().writeLong(rsrcId);
                        req.out().writeInt(buf.entriesCnt.get());
                        req.out().write(buf.out.array(), 0, buf.out.position());
                    },
                    null
                ).handle((res, err) -> {
                    if (err != null) {
                        failCntr.incrementAndGet();

                        fut.completeExceptionally(err);
                    }
                    else
                        fut.complete(null);

                    decrementPendingReqs();

                    return null;
                });

                buf.reinit();
            }
            catch (Throwable e) {
                failCntr.incrementAndGet();

                decrementPendingReqs();

                throw e;
            }
            finally {
                bufSendLock.unlock();
            }
        }

        /** */
        private void incrementPendingReqs(long timeout) throws ClientException {
            long startNanos = System.nanoTime();

            synchronized (pendingReqsMux) {
                long passedMillis = 0L;

                while (pendingReqsCnt >= MAX_PENDING_REQS && passedMillis < timeout) {
                    try {
                        pendingReqsMux.wait(timeout - passedMillis);

                        passedMillis = U.millisSinceNanos(startNanos);
                    }
                    catch (InterruptedException e) {
                        throw new ClientException(e);
                    }
                }

                if (passedMillis >= timeout) {
                    throw new ClientException(new TimeoutException("Data streamer exceeded timeout " +
                            "while waiting for pending requests delivery."));
                }

                pendingReqsCnt++;
            }
        }

        /** */
        private void decrementPendingReqs() {
            synchronized (pendingReqsMux) {
                pendingReqsCnt--;

                if (pendingReqsCnt == MAX_PENDING_REQS - 1 || pendingReqsCnt == 0)
                    pendingReqsMux.notifyAll();
            }
        }

        /** {@inheritDoc} */
        @Override public void flush()
                throws CacheException, IgniteInterruptedException, IllegalStateException, IgniteDataStreamerTimeoutException {
            flush0(timeout);
        }

        /** Flush without timeout. */
        private void flush0() {
            flush0(Long.MAX_VALUE);
        }

        /** */
        private void flush0(long timeout)
            throws CacheException, IgniteInterruptedException, IllegalStateException, IgniteDataStreamerTimeoutException {
            long startNanos = System.nanoTime();

            lockWithTimeout(bufLock, timeout);

            try {
                checkClosed();

                if (pendingBuf[0].entriesCnt.get() == 0 && pendingBuf[0].flushed &&
                    pendingBuf[1].entriesCnt.get() == 0 && pendingBuf[1].flushed)
                    return;

                long passedMillis = U.millisSinceNanos(startNanos);

                sendData(timeout - passedMillis);

                // Wait for delivery of all pending "add" requests.
                synchronized (pendingReqsMux) {
                    while (pendingReqsCnt > 0 && passedMillis < timeout) {
                        pendingReqsMux.wait(timeout - passedMillis);

                        passedMillis = U.millisSinceNanos(startNanos);
                    }
                }

                if (passedMillis >= timeout) {
                    throw new ClientException(new TimeoutException("Data streamer exceeded timeout " +
                            "while waiting for all pending requests delivery."));
                }

                CompletableFuture<?> fut = ch.serviceAsync(
                        ClientOperation.DATA_STREAMER_FLUSH,
                        req -> req.out().writeLong(rsrcId),
                        null
                );

                fut.handle((res, err) -> {
                    if (err != null)
                        failCntr.incrementAndGet();

                    return null;
                });

                fut.get(timeout - passedMillis, TimeUnit.MILLISECONDS);

                pendingBuf[0].flushed = true;
                pendingBuf[1].flushed = true;
            }
            catch (TimeoutException e) {
                throw new ClientException("Data streamer exceeded timeout while waiting for reply for flush operation.", e);
            } catch (InterruptedException | ExecutionException e) {
                failCntr.incrementAndGet();

                // TODO Correct exception unwrapping for IgniteCheckedException
                throw new ClientException(e);
            } finally {
                bufLock.unlock();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override public void tryFlush() throws CacheException, IgniteInterruptedException, IllegalStateException {
            checkClosed();

            executor.submit((Runnable)this::flush0);
        }

        /**
         * {@inheritDoc}
         */
        @Override public void close(boolean cancel)
            throws CacheException, IgniteInterruptedException, IgniteDataStreamerTimeoutException {
            long startNanos = System.nanoTime();

            lockWithTimeout(bufLock, timeout);

            try {
                if (closed)
                    return;

                Throwable err = null;

                try {
                    if (!cancel)
                        flush0(timeout - U.millisSinceNanos(startNanos));
                }
                catch (Throwable e) {
                    err = e;

                    throw e;
                } finally {
                    try {
                        ch.service(ClientOperation.RESOURCE_CLOSE, req -> req.out().writeLong(rsrcId), null);
                    }
                    catch (ClientConnectionException ignore) {
                        // No-op.
                    }

                    closed = true;

                    bufSwitchCondition.signalAll();

                    if (err == null && failCntr.get() > 0) {
                        err = new ClientException("Some of DataStreamer operations failed [failedCount=" +
                                failCntr.get() + "]");
                    }

                    if (err == null)
                        streamerFut.complete(null);
                    else
                        streamerFut.completeExceptionally(err);
                }
            } finally {
                bufLock.unlock();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override public void close()
            throws CacheException, IgniteInterruptedException, IgniteDataStreamerTimeoutException {
            close(false);
        }

        /** */
        private void lockWithTimeout(Lock lock, long timeout) {
            try {
                if (!lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                    // TODO make correct exception for timeout.
                    throw new ClientException(new TimeoutException("Data streamer exceeded timeout " +
                            "while waiting for acquire lock."));
                }
            }
            catch (InterruptedException e) {
                throw new ClientException(e);
            }
        }

        /**
         * Check if streamer is closed.
         */
        private void checkClosed() throws ClientException {
            if (closed)
                throw new ClientException("Data streamer is closed");
        }
    }

    /**
     * Buffer of pending entries to send.
     */
    private static class PendingBuffer {
        /** Internal future. Completes when buffer entries is delivered to server. */
        private CompletableFuture<Void> intFut;

        /** Public future wrapper for {@code intFut}. */
        private IgniteClientFuture<Void> pubFut;

        /** Count of entries in the buffer. */
        private final AtomicInteger entriesCnt = new AtomicInteger();

        /** Output streams. */
        private final BinaryOutputStream out;

        /** Writers to output streams. */
        private final BinaryRawWriterEx writer;

        /** Is buffer scheduled to be sent. */
        private final AtomicBoolean sendScheduled = new AtomicBoolean();

        /** Is buffer scheduled to be flushed. */
        private final AtomicBoolean flushScheduled = new AtomicBoolean();

        /** Is buffer flushed. */
        private boolean flushed;

        /**
         * @param utils Serialization/deserialization utils.
         */
        private PendingBuffer(ClientUtils utils) {
            out = new BinaryHeapOutputStream(1024);

            writer = utils.createBinaryWriter(out);

            reinit();

            flushed = true;
        }

        /**
         * Reinit buffer after send to reuse it.
         */
        private void reinit() {
            intFut = new CompletableFuture<>();

            pubFut = new IgniteClientFutureImpl<>(intFut);

            entriesCnt.set(0);

            out.position(0);

            sendScheduled.set(false);

            flushScheduled.set(false);

            flushed = false;
        }
    }
}