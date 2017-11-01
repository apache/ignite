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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;
import sun.nio.ch.DirectBuffer;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MSG_WRITER;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.NIO_OPERATION;

/**
 * Thread performing only read operations from the channel.
 */
abstract class AbstractNioClientWorker<T> extends GridWorker implements GridNioWorker {
    /** For test purpose. */
    protected boolean skipRead;

    /** For test purpose. */
    protected boolean skipWrite;

    /** Queue of change requests on this selector. */
    @GridToStringExclude
    private final ConcurrentLinkedQueue<SessionChangeRequest> changeReqs = new ConcurrentLinkedQueue<>();

    /** */
    @GridToStringExclude
    protected final GridNioServer<T> nio;

    /** */
    @GridToStringExclude
    protected final GridNioFilterChain<T> filterChain;

    /** Selector to select read events. */
    @GridToStringExclude
    private Selector selector;

    /** Selected keys. */
    @GridToStringExclude
    private SelectedSelectionKeySet selectedKeys;

    /** Workers sessions. */
    @GridToStringExclude
    private final Set<GridSelectorNioSessionImpl> sessions = new GridConcurrentHashSet<>();

    /** Worker index. */
    private final int idx;

    /** Total bytes received by this worker. */
    private long bytesRcvd;

    /** Total bytes sent by this worker. */
    private long bytesSent;

    /** Bytes received by this worker since rebalancing. */
    private volatile long bytesRcvd0;

    /** Bytes sent by this worker since rebalancing. */
    private volatile long bytesSent0;

    /** {@code True} if worker has called or is about to call {@code Selector.select()}. */
    private volatile boolean select;

    /** */
    private final long selectorSpins;

    /** */
    private final long writeTimeout;

    /** */
    private final long idleTimeout;

    /** */
    @GridToStringExclude
    private final boolean directMode;

    /** */
    @GridToStringExclude
    private final boolean directBuf;

    /** */
    @GridToStringExclude
    private final ByteOrder order;

    /** */
    @GridToStringExclude
    private final int sndQueueLimit;

    /**
     * @param idx Index of this worker in server's array.
     * @param igniteInstanceName Ignite instance name.
     * @param name Worker name.
     * @param filterChain Filter chain.
     * @param selectorSpins Selector spins count.
     * @param writeTimeout Write timeout.
     * @param idleTimeout Idle timeout.
     * @param directMode Is the worker is a direct worker.
     * @param directBuf Is the worker uses a direct ByteBuffer.
     * @param order Byte order for ByteBuffer.
     * @param sndQueueLimit Session send queue limit.
     * @param log Logger.
     * @throws IgniteCheckedException If selector could not be created.
     */
    protected AbstractNioClientWorker(GridNioServer<T> nio,
        int idx,
        String igniteInstanceName,
        String name,
        GridNioFilterChain<T> filterChain,
        long selectorSpins,
        long writeTimeout,
        long idleTimeout,
        boolean directMode,
        boolean directBuf,
        ByteOrder order,
        int sndQueueLimit,
        IgniteLogger log) throws IgniteCheckedException {
        super(igniteInstanceName, name, log);
        this.nio = nio;
        this.selectorSpins = selectorSpins;
        this.writeTimeout = writeTimeout;
        this.idleTimeout = idleTimeout;
        this.filterChain = filterChain;
        this.directMode = directMode;
        this.directBuf = directBuf;
        this.order = order;
        this.sndQueueLimit = sndQueueLimit;

        createSelector();

        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public <R extends GridNioSession> Set<R> sessions() {
        return (Set<R>)sessions;
    }

    /** {@inheritDoc} */
    @Override public long bytesReceivedSinceRebalancing() {
        return bytesRcvd0;
    }

    /** {@inheritDoc} */
    @Override public long bytesSentSinceRebalancing() {
        return bytesSent0;
    }

    /** {@inheritDoc} */
    @Override public long bytesReceivedTotal() {
        return bytesRcvd;
    }

    /** {@inheritDoc} */
    @Override public long bytesSentTotal() {
        return bytesSent;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        try {
            boolean reset = false;

            while (!isCancelled) {
                try {
                    if (reset)
                        createSelector();

                    bodyInternal();
                }
                catch (IgniteCheckedException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        U.error(log, "Failed to read data from remote connection (will wait for " +
                            GridNioServer.ERR_WAIT_TIME + "ms).", e);

                        U.sleep(GridNioServer.ERR_WAIT_TIME);

                        reset = true;
                    }
                }
            }
        }
        catch (Throwable e) {
            U.error(log, "Caught unhandled exception in NIO worker thread (restart the node).", e);

            if (e instanceof Error)
                throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void offer(SessionChangeRequest req) {
        changeReqs.offer(req);

        if (select)
            selector.wakeup();
    }

    /** {@inheritDoc} */
    @Override public void offer(Collection<SessionChangeRequest> reqs) {
        for (SessionChangeRequest req : reqs)
            changeReqs.offer(req);

        selector.wakeup();
    }

    /** {@inheritDoc} */
    @Override public List<SessionChangeRequest> clearSessionRequests(GridNioSession ses) {
        List<SessionChangeRequest> sesReqs = null;

        for (SessionChangeRequest changeReq : changeReqs) {
            if (changeReq.session() == ses && !(changeReq instanceof SessionMoveFuture)) {
                boolean rmv = changeReqs.remove(changeReq);

                assert rmv : changeReq;

                if (sesReqs == null)
                    sesReqs = new ArrayList<>();

                sesReqs.add(changeReq);
            }
        }

        return sesReqs;
    }

    /** {@inheritDoc} */
    @Override public final void registerWrite(GridSelectorNioSessionImpl ses) {
        SelectionKey key = ses.key();

        if (key.isValid()) {
            if ((key.interestOps() & SelectionKey.OP_WRITE) == 0)
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

            // Update timestamp to protected against false write timeout.
            ses.bytesSent(0);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void createSelector() throws IgniteCheckedException {
        selectedKeys = null;

        selector = nio.createSelector(null);

        if (GridNioServer.DISABLE_KEYSET_OPTIMIZATION)
            return;

        try {

            Class<?> selectorImplCls =
                Class.forName("sun.nio.ch.SelectorImpl", false, U.gridClassLoader());

            // Ensure the current selector implementation is what we can instrument.
            if (!selectorImplCls.isAssignableFrom(selector.getClass()))
                return;

            Field selectedKeysField = selectorImplCls.getDeclaredField("selectedKeys");
            Field publicSelectedKeysField = selectorImplCls.getDeclaredField("publicSelectedKeys");

            selectedKeysField.setAccessible(true);
            publicSelectedKeysField.setAccessible(true);

            SelectedSelectionKeySet selectedKeySet = new SelectedSelectionKeySet();

            selectedKeysField.set(selector, selectedKeySet);
            publicSelectedKeysField.set(selector, selectedKeySet);

            selectedKeys = selectedKeySet;

            if (log.isDebugEnabled())
                log.debug("Instrumented an optimized java.util.Set into: " + selector);
        }
        catch (Exception e) {
            selectedKeys = null;

            if (log.isDebugEnabled())
                log.debug("Failed to instrument an optimized java.util.Set into selector [selector=" + selector
                    + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public final void reset() {
        bytesSent0 = 0;
        bytesRcvd0 = 0;

        for (GridNioSession ses : sessions())
            ses.reset();
    }

    /**
     * For test purpose.
     * @param skipRead True if the worker should skip socket read operation.
     */
    final void skipRead(boolean skipRead) {
        this.skipRead = skipRead;
    }

    /**
     * For test purpose.
     * @param skipWrite True if the worker should skip socket write operation.
     */
    final void skipWrite(boolean skipWrite) {
        this.skipWrite = skipWrite;
    }

    /**
     * @param cnt Bytes read.
     */
    final void onRead(int cnt) {
        bytesRcvd += cnt;
        bytesRcvd0 += cnt;
    }

    /**
     * @param cnt Bytes write.
     */
    final void onWrite(int cnt) {
        bytesSent += cnt;
        bytesSent0 += cnt;
    }

    /**
     * Processes read-available event on the key.
     *
     * @param key Key that is ready to be read.
     * @throws IOException If key read failed.
     */
    protected abstract void processRead(SelectionKey key) throws IOException;

    /**
     * Processes write-ready event on the key.
     *
     * @param key Key that is ready to be written.
     * @throws IOException If write failed.
     */
    protected abstract void processWrite(SelectionKey key) throws IOException;

    /**
     * Processes read and write events and registration requests.
     *
     * @throws IgniteCheckedException If IOException occurred or thread was unable to add worker to workers pool.
     */
    @SuppressWarnings("unchecked")
    private void bodyInternal() throws IgniteCheckedException, InterruptedException {
        try {
            long lastIdleCheck = U.currentTimeMillis();

            mainLoop:
            while (!isCancelled && selector.isOpen()) {
                SessionChangeRequest req0;

                while ((req0 = changeReqs.poll()) != null) {
                    switch (req0.operation()) {
                        case CONNECT: {
                            NioOperationFuture fut = (NioOperationFuture)req0;

                            SocketChannel ch = fut.socketChannel();

                            try {
                                ch.register(selector, SelectionKey.OP_CONNECT, fut);
                            }
                            catch (IOException e) {
                                fut.onDone(new IgniteCheckedException("Failed to register channel on selector", e));
                            }

                            break;
                        }

                        case CANCEL_CONNECT: {
                            NioOperationFuture req = (NioOperationFuture)req0;

                            SocketChannel ch = req.socketChannel();

                            SelectionKey key = ch.keyFor(selector);

                            if (key != null)
                                key.cancel();

                            U.closeQuiet(ch);

                            req.onDone();

                            break;
                        }

                        case REGISTER: {
                            register((NioOperationFuture)req0);

                            break;
                        }

                        case MOVE: {
                            SessionMoveFuture f = (SessionMoveFuture)req0;

                            GridSelectorNioSessionImpl ses = f.session();

                            if (idx == f.toIndex()) {
                                assert f.movedSocketChannel() != null : f;

                                boolean add = sessions().add(ses);

                                assert add;

                                ses.finishMoveSession(this);

                                if (idx % 2 == 0)
                                    nio.incrementReaderMovedCount();
                                else
                                    nio.incrementWriterMovedCount();

                                SelectionKey key = f.movedSocketChannel().register(selector,
                                    SelectionKey.OP_READ | SelectionKey.OP_WRITE,
                                    ses);

                                ses.key(key);

                                ses.procWrite.set(true);

                                f.onDone(true);
                            }
                            else {
                                assert f.movedSocketChannel() == null : f;

                                if (sessions().remove(ses)) {
                                    ses.startMoveSession(this);

                                    SelectionKey key = ses.key();

                                    assert key.channel() != null : key;

                                    f.movedSocketChannel((SocketChannel)key.channel());

                                    key.cancel();

                                    ses.reset();

                                    nio.workers().get(f.toIndex()).offer(f);
                                }
                                else
                                    f.onDone(false);
                            }

                            break;
                        }

                        case REQUIRE_WRITE: {
                            SessionWriteRequest req = (SessionWriteRequest)req0;

                            registerWrite((GridSelectorNioSessionImpl)req.session());

                            break;
                        }

                        case CLOSE: {
                            NioOperationFuture req = (NioOperationFuture)req0;

                            if (close(req.session(), null))
                                req.onDone(true);
                            else
                                req.onDone(false);

                            break;
                        }

                        case PAUSE_READ: {
                            NioOperationFuture req = (NioOperationFuture)req0;

                            SelectionKey key = req.session().key();

                            if (key.isValid()) {
                                key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));

                                GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

                                ses.readsPaused(true);

                                req.onDone(true);
                            }
                            else
                                req.onDone(false);

                            break;
                        }

                        case RESUME_READ: {
                            NioOperationFuture req = (NioOperationFuture)req0;

                            SelectionKey key = req.session().key();

                            if (key.isValid()) {
                                key.interestOps(key.interestOps() | SelectionKey.OP_READ);

                                GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

                                ses.readsPaused(false);

                                req.onDone(true);
                            }
                            else
                                req.onDone(false);

                            break;
                        }

                        case DUMP_STATS: {
                            NioOperationFuture req = (NioOperationFuture)req0;

                            IgnitePredicate<GridNioSession> p =
                                req.message() instanceof IgnitePredicate ? (IgnitePredicate<GridNioSession>)req.message() : null;

                            StringBuilder sb = new StringBuilder();

                            try {
                                dumpStats(sb, p, p != null);
                            }
                            finally {
                                req.onDone(sb.toString());
                            }
                        }
                    }
                }

                int res = 0;

                for (long i = 0; i < selectorSpins && res == 0; i++) {
                    res = selector.selectNow();

                    if (res > 0) {
                        // Walk through the ready keys collection and process network events.
                        if (selectedKeys == null)
                            processSelectedKeys(selector.selectedKeys());
                        else
                            processSelectedKeysOptimized(selectedKeys.flip());
                    }

                    if (!changeReqs.isEmpty())
                        continue mainLoop;

                    // Just in case we do busy selects.
                    long now = U.currentTimeMillis();

                    if (now - lastIdleCheck > 2000) {
                        lastIdleCheck = now;

                        checkIdle(selector.keys());
                    }

                    if (isCancelled())
                        return;
                }

                // Falling to blocking select.
                select = true;

                try {
                    if (!changeReqs.isEmpty())
                        continue;

                    // Wake up every 2 seconds to check if closed.
                    if (selector.select(2000) > 0) {
                        // Walk through the ready keys collection and process network events.
                        if (selectedKeys == null)
                            processSelectedKeys(selector.selectedKeys());
                        else
                            processSelectedKeysOptimized(selectedKeys.flip());
                    }
                }
                finally {
                    select = false;
                }

                long now = U.currentTimeMillis();

                if (now - lastIdleCheck > 2000) {
                    lastIdleCheck = now;

                    checkIdle(selector.keys());
                }
            }
        }
        // Ignore this exception as thread interruption is equal to 'close' call.
        catch (ClosedByInterruptException e) {
            if (log.isDebugEnabled())
                log.debug("Closing selector due to thread interruption: " + e.getMessage());
        }
        catch (ClosedSelectorException e) {
            throw new IgniteCheckedException("Selector got closed while active.", e);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to select events on selector.", e);
        }
        finally {
            if (selector.isOpen()) {
                if (log.isDebugEnabled())
                    log.debug("Closing all connected client sockets.");

                // Close all channels registered with selector.
                for (SelectionKey key : selector.keys()) {
                    GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                    if (attach != null && attach.hasSession())
                        close(attach.session(), null);
                }

                if (log.isDebugEnabled())
                    log.debug("Closing NIO selector.");

                U.close(selector, log);
            }
        }
    }

    /**
     * @param sb Message builder.
     * @param keys Keys.
     */
    private void dumpSelectorInfo(StringBuilder sb, Set<SelectionKey> keys) {
        sb.append(">> Selector info [idx=").append(idx)
            .append(", keysCnt=").append(keys.size())
            .append(", bytesRcvd=").append(bytesReceivedTotal())
            .append(", bytesRcvd0=").append(bytesReceivedSinceRebalancing())
            .append(", bytesSent=").append(bytesSentTotal())
            .append(", bytesSent0=").append(bytesSentSinceRebalancing())
            .append("]").append(U.nl());
    }

    /**
     * @param sb Message builder.
     * @param p Optional session predicate.
     * @param shortInfo Short info flag.
     */
    private void dumpStats(StringBuilder sb,
        @Nullable IgnitePredicate<GridNioSession> p,
        boolean shortInfo) {
        Set<SelectionKey> keys = selector.keys();

        boolean selInfo = p == null;

        if (selInfo)
            dumpSelectorInfo(sb, keys);

        for (SelectionKey key : keys) {
            GridSelectorNioSessionImpl ses = (GridSelectorNioSessionImpl)key.attachment();

            boolean sesInfo = p == null || p.apply(ses);

            if (sesInfo) {
                if (!selInfo) {
                    dumpSelectorInfo(sb, keys);

                    selInfo = true;
                }

                sb.append("    Connection info [")
                    .append("in=").append(ses.accepted())
                    .append(", rmtAddr=").append(ses.remoteAddress())
                    .append(", locAddr=").append(ses.localAddress());

                GridNioRecoveryDescriptor outDesc = ses.outRecoveryDescriptor();

                if (outDesc != null) {
                    sb.append(", msgsSent=").append(outDesc.sent())
                        .append(", msgsAckedByRmt=").append(outDesc.acked())
                        .append(", descIdHash=").append(System.identityHashCode(outDesc));

                    if (!outDesc.messagesRequests().isEmpty()) {
                        int cnt = 0;

                        sb.append(", unackedMsgs=[");

                        for (SessionWriteRequest req : outDesc.messagesRequests()) {
                            if (cnt != 0)
                                sb.append(", ");

                            Object msg = req.message();

                            if (shortInfo && msg instanceof GridIoMessage)
                                msg = ((GridIoMessage)msg).message().getClass().getSimpleName();

                            sb.append(msg);

                            if (++cnt == 5)
                                break;
                        }

                        sb.append(']');
                    }
                }
                else
                    sb.append(", outRecoveryDesc=null");

                GridNioRecoveryDescriptor inDesc = ses.inRecoveryDescriptor();

                if (inDesc != null) {
                    sb.append(", msgsRcvd=").append(inDesc.received())
                        .append(", lastAcked=").append(inDesc.lastAcknowledged())
                        .append(", descIdHash=").append(System.identityHashCode(inDesc));
                }
                else
                    sb.append(", inRecoveryDesc=null");

                sb.append(", bytesRcvd=").append(ses.bytesReceivedTotal())
                    .append(", bytesRcvd0=").append(ses.bytesReceivedSinceRebalancing())
                    .append(", bytesSent=").append(ses.bytesSentTotal())
                    .append(", bytesSent0=").append(ses.bytesSentSinceRebalancing())
                    .append(", opQueueSize=").append(ses.writeQueueSize());

                if (!shortInfo) {
                    MessageWriter writer = ses.meta(MSG_WRITER.ordinal());
                    MessageReader reader = ses.meta(GridDirectParser.READER_META_KEY);

                    sb.append(", msgWriter=").append(writer != null ? writer.toString() : "null")
                        .append(", msgReader=").append(reader != null ? reader.toString() : "null");
                }

                int cnt = 0;

                for (SessionWriteRequest req : ses.writeQueue()) {
                    Object msg = req.message();

                    if (shortInfo && msg instanceof GridIoMessage)
                        msg = ((GridIoMessage)msg).message().getClass().getSimpleName();

                    if (cnt == 0)
                        sb.append(",\n opQueue=[").append(msg);
                    else
                        sb.append(',').append(msg);

                    if (++cnt == 5) {
                        sb.append(']');

                        break;
                    }
                }

                sb.append("]");
            }
        }
    }

    /**
     * Processes keys selected by a selector.
     *
     * @param keys Selected keys.
     * @throws ClosedByInterruptException If this thread was interrupted while reading data.
     */
    private void processSelectedKeysOptimized(SelectionKey[] keys) throws ClosedByInterruptException {
        for (int i = 0; ; i++) {
            final SelectionKey key = keys[i];

            if (key == null)
                break;

            // null out entry in the array to allow to have it GC'ed once the Channel close
            // See https://github.com/netty/netty/issues/2363
            keys[i] = null;

            // Was key closed?
            if (!key.isValid())
                continue;

            GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

            assert attach != null;

            try {
                if (!attach.hasSession() && key.isConnectable()) {
                    processConnect(key);

                    continue;
                }

                if (key.isReadable())
                    processRead(key);

                if (key.isValid() && key.isWritable())
                    processWrite(key);
            }
            catch (ClosedByInterruptException e) {
                // This exception will be handled in bodyInternal() method.
                throw e;
            }
            catch (Exception | Error e) { // TODO IGNITE-2659.
                try {
                    U.sleep(1000);
                }
                catch (IgniteInterruptedCheckedException ignore) {
                    // No-op.
                }

                GridSelectorNioSessionImpl ses = attach.session();

                if (!isCancelled)
                    U.error(log, "Failed to process selector key [ses=" + ses + ']', e);
                else if (log.isDebugEnabled())
                    log.debug("Failed to process selector key [ses=" + ses + ", err=" + e + ']');

                close(ses, new GridNioException(e));
            }
        }
    }

    /**
     * Processes keys selected by a selector.
     *
     * @param keys Selected keys.
     * @throws ClosedByInterruptException If this thread was interrupted while reading data.
     */
    private void processSelectedKeys(Set<SelectionKey> keys) throws ClosedByInterruptException {
        if (log.isTraceEnabled())
            log.trace("Processing keys in client worker: " + keys.size());

        if (keys.isEmpty())
            return;

        for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext(); ) {
            SelectionKey key = iter.next();

            iter.remove();

            // Was key closed?
            if (!key.isValid())
                continue;

            GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

            assert attach != null;

            try {
                if (!attach.hasSession() && key.isConnectable()) {
                    processConnect(key);

                    continue;
                }

                if (key.isReadable())
                    processRead(key);

                if (key.isValid() && key.isWritable())
                    processWrite(key);
            }
            catch (ClosedByInterruptException e) {
                // This exception will be handled in bodyInternal() method.
                throw e;
            }
            catch (Exception | Error e) { // TODO IGNITE-2659.
                try {
                    U.sleep(1000);
                }
                catch (IgniteInterruptedCheckedException ignore) {
                    // No-op.
                }

                GridSelectorNioSessionImpl ses = attach.session();

                if (!isCancelled)
                    U.error(log, "Failed to process selector key [ses=" + ses + ']', e);
                else if (log.isDebugEnabled())
                    log.debug("Failed to process selector key [ses=" + ses + ", err=" + e + ']');
            }
        }
    }

    /**
     * Checks sessions assigned to a selector for timeouts.
     *
     * @param keys Keys registered to selector.
     */
    private void checkIdle(Iterable<SelectionKey> keys) {
        long now = U.currentTimeMillis();

        for (SelectionKey key : keys) {
            GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

            if (attach == null || !attach.hasSession())
                continue;

            GridSelectorNioSessionImpl ses = attach.session();

            try {

                boolean opWrite = key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0;

                // If we are writing and timeout passed.
                if (opWrite && now - ses.lastSendTime() > writeTimeout) {
                    filterChain.onSessionWriteTimeout(ses);

                    // Update timestamp to avoid multiple notifications within one timeout interval.
                    ses.bytesSent(0);

                    continue;
                }

                if (!opWrite &&
                    now - ses.lastReceiveTime() > idleTimeout &&
                    now - ses.lastSendScheduleTime() > idleTimeout) {
                    filterChain.onSessionIdleTimeout(ses);

                    // Update timestamp to avoid multiple notifications within one timeout interval.
                    ses.resetSendScheduleTime();
                    ses.bytesReceived(0);
                }
            }
            catch (IgniteCheckedException e) {
                close(ses, e);
            }
        }
    }

    /**
     * Registers given socket channel to the selector, creates a session and notifies the listener.
     *
     * @param fut Registration future.
     */
    private void register(NioOperationFuture<GridNioSession> fut) {
        assert fut != null;

        SocketChannel sockCh = fut.socketChannel();

        assert sockCh != null;

        Socket sock = sockCh.socket();

        try {
            ByteBuffer writeBuf = null;
            ByteBuffer readBuf = null;

            if (directMode) {
                writeBuf = directBuf ? ByteBuffer.allocateDirect(sock.getSendBufferSize()) :
                    ByteBuffer.allocate(sock.getSendBufferSize());
                readBuf = directBuf ? ByteBuffer.allocateDirect(sock.getReceiveBufferSize()) :
                    ByteBuffer.allocate(sock.getReceiveBufferSize());

                writeBuf.order(order);
                readBuf.order(order);
            }

            final GridSelectorNioSessionImpl ses = new GridSelectorNioSessionImpl(
                log,
                this,
                filterChain,
                (InetSocketAddress)sockCh.getLocalAddress(),
                (InetSocketAddress)sockCh.getRemoteAddress(),
                fut.accepted(),
                sndQueueLimit,
                writeBuf,
                readBuf);

            Map<Integer, ?> meta = fut.meta();

            if (meta != null) {
                for (Map.Entry<Integer, ?> e : meta.entrySet())
                    ses.addMeta(e.getKey(), e.getValue());

                if (!ses.accepted()) {
                    GridNioRecoveryDescriptor desc =
                        (GridNioRecoveryDescriptor)meta.get(GridNioServer.RECOVERY_DESC_META_KEY);

                    if (desc != null) {
                        ses.outRecoveryDescriptor(desc);

                        if (!desc.pairedConnections())
                            ses.inRecoveryDescriptor(desc);
                    }
                }
            }

            SelectionKey key;

            if (!sockCh.isRegistered()) {
                assert fut.operation() == NioOperation.REGISTER : fut.operation();

                key = sockCh.register(selector, SelectionKey.OP_READ, ses);

                ses.key(key);

                nio.resend(ses);
            }
            else {
                assert fut.operation() == NioOperation.CONNECT : fut.operation();

                key = sockCh.keyFor(selector);

                key.attach(ses);

                key.interestOps(key.interestOps() & (~SelectionKey.OP_CONNECT));
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);

                ses.key(key);
            }

            nio.sessions().add(ses);
            sessions().add(ses);

            try {
                filterChain.onSessionOpened(ses);

                fut.onDone(ses);
            }
            catch (IgniteCheckedException e) {
                close(ses, e);

                fut.onDone(e);
            }

            if (isCancelled)
                ses.onServerStopped();
        }
        catch (ClosedChannelException e) {
            U.warn(log, "Failed to register accepted socket channel to selector (channel was closed): "
                + sock.getRemoteSocketAddress(), e);
        }
        catch (IOException e) {
            U.error(log, "Failed to get socket addresses.", e);
        }
    }

    /**
     * Closes the session and all associated resources, then notifies the listener.
     *
     * @param ses Session to be closed.
     * @param e Exception to be passed to the listener, if any.
     * @return {@code True} if this call closed the ses.
     */
    protected boolean close(final GridSelectorNioSessionImpl ses, @Nullable final IgniteCheckedException e) {
        if (e != null) {
            // Print stack trace only if has runtime exception in it's cause.
            if (e.hasCause(IOException.class))
                U.warn(log, "Closing NIO session because of unhandled exception [cls=" + e.getClass() +
                    ", msg=" + e.getMessage() + ']');
            else
                U.error(log, "Closing NIO session because of unhandled exception.", e);
        }

        nio.sessions().remove(ses);
        sessions().remove(ses);

        SelectionKey key = ses.key();

        if (ses.setClosed()) {
            ses.onClosed();

            if (directBuf) {
                if (ses.writeBuffer() != null)
                    ((DirectBuffer)ses.writeBuffer()).cleaner().clean();

                if (ses.readBuffer() != null)
                    ((DirectBuffer)ses.readBuffer()).cleaner().clean();
            }

            // Shutdown input and output so that remote client will see correct socket close.
            Socket sock = ((SocketChannel)key.channel()).socket();

            try {
                try {
                    sock.shutdownInput();
                }
                catch (IOException ignored) {
                    // No-op.
                }

                try {
                    sock.shutdownOutput();
                }
                catch (IOException ignored) {
                    // No-op.
                }
            }
            finally {
                U.close(key, log);
                U.close(sock, log);
            }

            if (e != null)
                filterChain.onExceptionCaught(ses, e);

            ses.removeMeta(GridNioServer.BUF_META_KEY);

            // Since ses is in closed state, no write requests will be added.
            SessionWriteRequest req = ses.removeMeta(NIO_OPERATION.ordinal());

            GridNioRecoveryDescriptor outRecovery = ses.outRecoveryDescriptor();
            GridNioRecoveryDescriptor inRecovery = ses.inRecoveryDescriptor();

            IOException err = new IOException("Failed to send message (connection was closed): " + ses);

            if (outRecovery != null || inRecovery != null) {
                try {
                    // Poll will update recovery data.
                    while ((req = ses.pollFuture()) != null) {
                        if (req.skipRecovery())
                            req.onError(err);
                    }
                }
                finally {
                    if (outRecovery != null)
                        outRecovery.release();

                    if (inRecovery != null && inRecovery != outRecovery)
                        inRecovery.release();
                }
            }
            else {
                if (req != null)
                    req.onError(err);

                while ((req = ses.pollFuture()) != null)
                    req.onError(err);
            }

            try {
                filterChain.onSessionClosed(ses);
            }
            catch (IgniteCheckedException e1) {
                filterChain.onExceptionCaught(ses, e1);
            }

            return true;
        }

        return false;
    }

    /**
     * @param key Key.
     * @throws IOException If failed.
     */
    @SuppressWarnings("unchecked")
    private void processConnect(SelectionKey key) throws IOException {
        SocketChannel ch = (SocketChannel)key.channel();

        NioOperationFuture<GridNioSession> sesFut = (NioOperationFuture<GridNioSession>)key.attachment();

        assert sesFut != null;

        try {
            if (ch.finishConnect())
                register(sesFut);
        }
        catch (IOException e) {
            U.closeQuiet(ch);

            sesFut.onDone(new GridNioException("Failed to connect to node", e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractNioClientWorker.class, this, super.toString());
    }
}
