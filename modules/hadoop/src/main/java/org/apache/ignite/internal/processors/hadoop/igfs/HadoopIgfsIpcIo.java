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

package org.apache.ignite.internal.processors.hadoop.igfs;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.logging.Log;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.igfs.common.IgfsControlResponse;
import org.apache.ignite.internal.igfs.common.IgfsDataInputStream;
import org.apache.ignite.internal.igfs.common.IgfsDataOutputStream;
import org.apache.ignite.internal.igfs.common.IgfsIpcCommand;
import org.apache.ignite.internal.igfs.common.IgfsMarshaller;
import org.apache.ignite.internal.igfs.common.IgfsMessage;
import org.apache.ignite.internal.igfs.common.IgfsStreamControlRequest;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcEndpointFactory;
import org.apache.ignite.internal.util.ipc.shmem.IpcOutOfSystemResourcesException;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * IO layer implementation based on blocking IPC streams.
 */
@SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
public class HadoopIgfsIpcIo implements HadoopIgfsIo {
    /** Logger. */
    private final Log log;

    /** Request futures map. */
    private ConcurrentMap<Long, HadoopIgfsFuture> reqMap =
        new ConcurrentHashMap8<>();

    /** Request ID counter. */
    private AtomicLong reqIdCnt = new AtomicLong();

    /** Endpoint. */
    private IpcEndpoint endpoint;

    /** Endpoint output stream. */
    private IgfsDataOutputStream out;

    /** Protocol. */
    private final IgfsMarshaller marsh;

    /** Client reader thread. */
    private Thread reader;

    /** Lock for graceful shutdown. */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Stopping flag. */
    private volatile boolean stopping;

    /** Server endpoint address. */
    private final String endpointAddr;

    /** Number of open file system sessions. */
    private final AtomicInteger activeCnt = new AtomicInteger(1);

    /** Event listeners. */
    private final Collection<HadoopIgfsIpcIoListener> lsnrs =
        new GridConcurrentHashSet<>();

    /** Cached connections. */
    private static final ConcurrentMap<String, HadoopIgfsIpcIo> ipcCache =
        new ConcurrentHashMap8<>();

    /** Striped lock that prevents multiple instance creation in {@link #get(Log, String)}. */
    private static final GridStripedLock initLock = new GridStripedLock(32);

    /**
     * @param endpointAddr Endpoint.
     * @param marsh Protocol.
     * @param log Logger to use.
     */
    public HadoopIgfsIpcIo(String endpointAddr, IgfsMarshaller marsh, Log log) {
        assert endpointAddr != null;
        assert marsh != null;

        this.endpointAddr = endpointAddr;
        this.marsh = marsh;
        this.log = log;
    }

    /**
     * Returns a started and valid instance of this class
     * for a given endpoint.
     *
     * @param log Logger to use for new instance.
     * @param endpoint Endpoint string.
     * @return New or existing cached instance, which is started and operational.
     * @throws IOException If new instance was created but failed to start.
     */
    public static HadoopIgfsIpcIo get(Log log, String endpoint) throws IOException {
        while (true) {
            HadoopIgfsIpcIo clientIo = ipcCache.get(endpoint);

            if (clientIo != null) {
                if (clientIo.acquire())
                    return clientIo;
                else
                    // If concurrent close.
                    ipcCache.remove(endpoint, clientIo);
            }
            else {
                Lock lock = initLock.getLock(endpoint);

                lock.lock();

                try {
                    clientIo = ipcCache.get(endpoint);

                    if (clientIo != null) { // Perform double check.
                        if (clientIo.acquire())
                            return clientIo;
                        else
                            // If concurrent close.
                            ipcCache.remove(endpoint, clientIo);
                    }

                    // Otherwise try creating a new one.
                    clientIo = new HadoopIgfsIpcIo(endpoint, new IgfsMarshaller(), log);

                    try {
                        clientIo.start();
                    }
                    catch (IgniteCheckedException e) {
                        throw new IOException(e.getMessage(), e);
                    }

                    HadoopIgfsIpcIo old = ipcCache.putIfAbsent(endpoint, clientIo);

                    // Put in exclusive lock.
                    assert old == null;

                    return clientIo;
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }

    /**
     * Increases usage count for this instance.
     *
     * @return {@code true} if usage count is greater than zero.
     */
    private boolean acquire() {
        while (true) {
            int cnt = activeCnt.get();

            if (cnt == 0) {
                if (log.isDebugEnabled())
                    log.debug("IPC IO not acquired (count was 0): " + this);

                return false;
            }

            // Need to make sure that no-one decremented count in between.
            if (activeCnt.compareAndSet(cnt, cnt + 1)) {
                if (log.isDebugEnabled())
                    log.debug("IPC IO acquired: " + this);

                return true;
            }
        }
    }

    /**
     * Releases this instance, decrementing usage count.
     * <p>
     * If usage count becomes zero, the instance is stopped
     * and removed from cache.
     */
    public void release() {
        while (true) {
            int cnt = activeCnt.get();

            if (cnt == 0) {
                if (log.isDebugEnabled())
                    log.debug("IPC IO not released (count was 0): " + this);

                return;
            }

            if (activeCnt.compareAndSet(cnt, cnt - 1)) {
                if (cnt == 1) {
                    ipcCache.remove(endpointAddr, this);

                    if (log.isDebugEnabled())
                        log.debug("IPC IO stopping as unused: " + this);

                    stop();
                }
                else if (log.isDebugEnabled())
                    log.debug("IPC IO released: " + this);

                return;
            }
        }
    }

    /**
     * Closes this IO instance, removing it from cache.
     */
    public void forceClose() {
        if (ipcCache.remove(endpointAddr, this))
            stop();
    }

    /**
     * Starts the IO.
     *
     * @throws IgniteCheckedException If failed to connect the endpoint.
     */
    private void start() throws IgniteCheckedException {
        boolean success = false;

        try {
            endpoint = IpcEndpointFactory.connectEndpoint(
                endpointAddr, new GridLoggerProxy(new HadoopIgfsJclLogger(log), null, null, ""));

            out = new IgfsDataOutputStream(new BufferedOutputStream(endpoint.outputStream()));

            reader = new ReaderThread();

            // Required for Hadoop 2.x
            reader.setDaemon(true);

            reader.start();

            success = true;
        }
        catch (IgniteCheckedException e) {
            IpcOutOfSystemResourcesException resEx = e.getCause(IpcOutOfSystemResourcesException.class);

            if (resEx != null)
                throw new IgniteCheckedException(IpcSharedMemoryServerEndpoint.OUT_OF_RESOURCES_MSG, resEx);

            throw e;
        }
        finally {
            if (!success)
                stop();
        }
    }

    /**
     * Shuts down the IO. No send requests will be accepted anymore, all pending futures will be failed.
     * Close listeners will be invoked as if connection is closed by server.
     */
    private void stop() {
        close0(null);

        if (reader != null) {
            try {
                U.interrupt(reader);
                U.join(reader);

                reader = null;
            }
            catch (IgniteInterruptedCheckedException ignored) {
                Thread.currentThread().interrupt();

                log.warn("Got interrupted while waiting for reader thread to shut down (will return).");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void addEventListener(HadoopIgfsIpcIoListener lsnr) {
        if (!busyLock.readLock().tryLock()) {
            lsnr.onClose();

            return;
        }

        boolean invokeNow = false;

        try {
            invokeNow = stopping;

            if (!invokeNow)
                lsnrs.add(lsnr);
        }
        finally {
            busyLock.readLock().unlock();

            if (invokeNow)
                lsnr.onClose();
        }
    }

    /** {@inheritDoc} */
    @Override public void removeEventListener(HadoopIgfsIpcIoListener lsnr) {
        lsnrs.remove(lsnr);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgfsMessage> send(IgfsMessage msg) throws IgniteCheckedException {
        return send(msg, null, 0, 0);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<T> send(IgfsMessage msg, @Nullable byte[] outBuf, int outOff,
        int outLen) throws IgniteCheckedException {
        assert outBuf == null || msg.command() == IgfsIpcCommand.READ_BLOCK;

        if (!busyLock.readLock().tryLock())
            throw new HadoopIgfsCommunicationException("Failed to send message (client is being concurrently " +
                "closed).");

        try {
            if (stopping)
                throw new HadoopIgfsCommunicationException("Failed to send message (client is being concurrently " +
                    "closed).");

            long reqId = reqIdCnt.getAndIncrement();

            HadoopIgfsFuture<T> fut = new HadoopIgfsFuture<>();

            fut.outputBuffer(outBuf);
            fut.outputOffset(outOff);
            fut.outputLength(outLen);
            fut.read(msg.command() == IgfsIpcCommand.READ_BLOCK);

            HadoopIgfsFuture oldFut = reqMap.putIfAbsent(reqId, fut);

            assert oldFut == null;

            if (log.isDebugEnabled())
                log.debug("Sending IGFS message [reqId=" + reqId + ", msg=" + msg + ']');

            byte[] hdr = IgfsMarshaller.createHeader(reqId, msg.command());

            IgniteCheckedException err = null;

            try {
                synchronized (this) {
                    marsh.marshall(msg, hdr, out);

                    out.flush(); // Blocking operation + sometimes system call.
                }
            }
            catch (IgniteCheckedException e) {
                err = e;
            }
            catch (IOException e) {
                err = new HadoopIgfsCommunicationException(e);
            }

            if (err != null) {
                reqMap.remove(reqId, fut);

                fut.onDone(err);
            }

            return fut;
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void sendPlain(IgfsMessage msg) throws IgniteCheckedException {
        if (!busyLock.readLock().tryLock())
            throw new HadoopIgfsCommunicationException("Failed to send message (client is being " +
                "concurrently closed).");

        try {
            if (stopping)
                throw new HadoopIgfsCommunicationException("Failed to send message (client is being concurrently closed).");

            assert msg.command() == IgfsIpcCommand.WRITE_BLOCK;

            IgfsStreamControlRequest req = (IgfsStreamControlRequest)msg;

            byte[] hdr = IgfsMarshaller.createHeader(-1, IgfsIpcCommand.WRITE_BLOCK);

            U.longToBytes(req.streamId(), hdr, 12);
            U.intToBytes(req.length(), hdr, 20);

            synchronized (this) {
                out.write(hdr);
                out.write(req.data(), (int)req.position(), req.length());

                out.flush();
            }
        }
        catch (IOException e) {
            throw new HadoopIgfsCommunicationException(e);
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Closes client but does not wait.
     *
     * @param err Error.
     */
    private void close0(@Nullable Throwable err) {
        busyLock.writeLock().lock();

        try {
            if (stopping)
                return;

            stopping = true;
        }
        finally {
            busyLock.writeLock().unlock();
        }

        if (err == null)
            err = new IgniteCheckedException("Failed to perform request (connection was concurrently closed before response " +
                "is received).");

        // Clean up resources.
        U.closeQuiet(out);

        if (endpoint != null)
            endpoint.close();

        // Unwind futures. We can safely iterate here because no more futures will be added.
        Iterator<HadoopIgfsFuture> it = reqMap.values().iterator();

        while (it.hasNext()) {
            HadoopIgfsFuture fut = it.next();

            fut.onDone(err);

            it.remove();
        }

        for (HadoopIgfsIpcIoListener lsnr : lsnrs)
            lsnr.onClose();
    }

    /**
     * Do not extend {@code GridThread} to minimize class dependencies.
     */
    private class ReaderThread extends Thread {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void run() {
            // Error to fail pending futures.
            Throwable err = null;

            try {
                InputStream in = endpoint.inputStream();

                IgfsDataInputStream dis = new IgfsDataInputStream(in);

                byte[] hdr = new byte[IgfsMarshaller.HEADER_SIZE];
                byte[] msgHdr = new byte[IgfsControlResponse.RES_HEADER_SIZE];

                while (!Thread.currentThread().isInterrupted()) {
                    dis.readFully(hdr);

                    long reqId = U.bytesToLong(hdr, 0);

                    // We don't wait for write responses, therefore reqId is -1.
                    if (reqId == -1) {
                        // We received a response which normally should not be sent. It must contain an error.
                        dis.readFully(msgHdr);

                        assert msgHdr[4] != 0;

                        String errMsg = dis.readUTF();

                        // Error code.
                        dis.readInt();

                        long streamId = dis.readLong();

                        for (HadoopIgfsIpcIoListener lsnr : lsnrs)
                            lsnr.onError(streamId, errMsg);
                    }
                    else {
                        HadoopIgfsFuture<Object> fut = reqMap.remove(reqId);

                        if (fut == null) {
                            String msg = "Failed to read response from server: response closure is unavailable for " +
                                "requestId (will close connection):" + reqId;

                            log.warn(msg);

                            err = new IgniteCheckedException(msg);

                            break;
                        }
                        else {
                            try {
                                IgfsIpcCommand cmd = IgfsIpcCommand.valueOf(U.bytesToInt(hdr, 8));

                                if (log.isDebugEnabled())
                                    log.debug("Received IGFS response [reqId=" + reqId + ", cmd=" + cmd + ']');

                                Object res = null;

                                if (fut.read()) {
                                    dis.readFully(msgHdr);

                                    boolean hasErr = msgHdr[4] != 0;

                                    if (hasErr) {
                                        String errMsg = dis.readUTF();

                                        // Error code.
                                        Integer errCode = dis.readInt();

                                        IgfsControlResponse.throwError(errCode, errMsg);
                                    }

                                    int blockLen = U.bytesToInt(msgHdr, 5);

                                    int readLen = Math.min(blockLen, fut.outputLength());

                                    if (readLen > 0) {
                                        assert fut.outputBuffer() != null;

                                        dis.readFully(fut.outputBuffer(), fut.outputOffset(), readLen);
                                    }

                                    if (readLen != blockLen) {
                                        byte[] buf = new byte[blockLen - readLen];

                                        dis.readFully(buf);

                                        res = buf;
                                    }
                                }
                                else
                                    res = marsh.unmarshall(cmd, hdr, dis);

                                fut.onDone(res);
                            }
                            catch (IgfsException | IgniteCheckedException e) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to apply response closure (will fail request future): " +
                                        e.getMessage());

                                fut.onDone(e);

                                err = e;
                            }
                            catch (Throwable t) {
                                fut.onDone(t);

                                throw t;
                            }
                        }
                    }
                }
            }
            catch (EOFException ignored) {
                err = new IgniteCheckedException("Failed to read response from server (connection was closed by remote peer).");
            }
            catch (IOException e) {
                if (!stopping)
                    log.error("Failed to read data (connection will be closed)", e);

                err = new HadoopIgfsCommunicationException(e);
            }
            catch (Throwable e) {
                if (!stopping)
                    log.error("Failed to obtain endpoint input stream (connection will be closed)", e);

                err = e;

                if (e instanceof Error)
                    throw (Error)e;
            }
            finally {
                close0(err);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [endpointAddr=" + endpointAddr + ", activeCnt=" + activeCnt +
            ", stopping=" + stopping + ']';
    }
}