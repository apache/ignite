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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.MpscQueue;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import sun.nio.ch.DirectBuffer;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.MSG_WRITER;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.NIO_OPERATION;

/**
 * Thread performing only read operations from the channel.
 */
@SuppressWarnings({"WeakerAccess", "ErrorNotRethrown", "FieldCanBeLocal"})
abstract class AbstractNioClientWorker<T> extends GridWorker implements GridNioWorker {
    /** */
    private static final int SELECT_TIMEOUT = 2000;

    /** For test purpose. */
    protected boolean skipRead;

    /** For test purpose. */
    protected boolean skipWrite;

    /** Queue of change requests on this selector. */
    @GridToStringExclude
    private final MpscQueue<SessionChangeRequest> reqs = new MpscQueue<>();

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

    /**
     * Select flag.
     * {@code false} means the worker is currently processing requests;
     * {@code true} means the worker is waiting on blocking select(..) operation.
     */
    @GridToStringExclude
    private final AtomicBoolean selectFlag = new AtomicBoolean();

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
    @Override public Selector selector() {
        return selector;
    }

    /** {@inheritDoc} */
    @Override public int idx() {
        return idx;
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
    @Override public void offer(SessionChangeRequest req)  {
        reqs.offer(req);

        if (selectFlag.get() && selectFlag.compareAndSet(true,false))
            selector.wakeup();
    }

    /** {@inheritDoc} */
    @Override public final void registerWrite(GridNioSession ses) {
        GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

        SelectionKey key = ses0.key();

        if (key.isValid()) {
            if ((key.interestOps() & SelectionKey.OP_WRITE) == 0)
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);

            // Update timestamp to protected against false write timeout.
            ses0.bytesSent(0);
        }
    }

    /** {@inheritDoc} */
    @Override public void dumpStats(StringBuilder sb, IgnitePredicate<GridNioSession> p, boolean shortInfo) {
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
                        sb.append(", unackedMsgs=[");

                        int cnt = 0;

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

                for (SessionWriteRequest req : ses.pendingRequests()) {
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

    /** {@inheritDoc} */
    @Override public void register(ConnectionOperationFuture fut) {
        assert fut != null;

        SocketChannel sockCh = fut.socketChannel();

        assert sockCh != null;

        Socket sock = sockCh.socket();

        try {
            final GridSelectorNioSessionImpl ses = newSession(fut);

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
                closeSession(ses, e);

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

    /** {@inheritDoc} */
    @Override public boolean closeSession(GridNioSession ses, IgniteCheckedException e) {
        final GridSelectorNioSessionImpl ses0 = (GridSelectorNioSessionImpl)ses;

        if (e != null) {
            // Print stack trace only if has runtime exception in it's cause.
            if (e.hasCause(IOException.class))
                U.warn(log, "Closing NIO session because of unhandled exception [cls=" + e.getClass() +
                    ", msg=" + e.getMessage() + ']');
            else
                U.error(log, "Closing NIO session because of unhandled exception.", e);
        }

        nio.sessions().remove(ses0);
        sessions().remove(ses0);

        SelectionKey key = ses0.key();

        if (ses0.setClosed()) {
            ses0.onClosed();

            if (directBuf) {
                if (ses0.writeBuffer() != null)
                    ((DirectBuffer)ses0.writeBuffer()).cleaner().clean();

                if (ses0.readBuffer() != null)
                    ((DirectBuffer)ses0.readBuffer()).cleaner().clean();
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
                filterChain.onExceptionCaught(ses0, e);

            ses0.removeMeta(GridNioServer.BUF_META_KEY);

            // Since ses is in closed state, no write requests will be added.
            SessionWriteRequest req = ses0.removeMeta(NIO_OPERATION.ordinal());

            GridNioRecoveryDescriptor outRecovery = ses0.outRecoveryDescriptor();
            GridNioRecoveryDescriptor inRecovery = ses0.inRecoveryDescriptor();

            IOException err = new IOException("Failed to send message (connection was closed): " + ses0);

            if (outRecovery != null || inRecovery != null) {
                try {
                    // Poll will update recovery data.
                    while ((req = ses0.pollFuture()) != null) {
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

                while ((req = ses0.pollFuture()) != null)
                    req.onError(err);
            }

            try {
                filterChain.onSessionClosed(ses0);
            }
            catch (IgniteCheckedException e1) {
                filterChain.onExceptionCaught(ses0, e1);
            }

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public final void reset() {
        bytesSent0 = 0;
        bytesRcvd0 = 0;

        for (GridNioSession ses : sessions())
            ses.reset();
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
     * @param cnt Read bytes count.
     */
    protected void onRead(int cnt) {
        bytesRcvd += cnt;
        bytesRcvd0 += cnt;
    }

    /**
     * @param cnt Write bytes count.
     */
    protected void onWrite(int cnt) {
        bytesSent += cnt;
        bytesSent0 += cnt;
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
     * @param size Buffer size.
     * @return Newly allocated byte buffer or null in case this worker is not "direct".
     */
    protected ByteBuffer allocateBuffer(int size) {
        ByteBuffer buffer = directBuf ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
        buffer.order(order);

        return buffer;
    }

    /**
     * @return True if at least one request was processed.
     * @throws IOException If failed.
     * @throws IgniteCheckedException If failed.
     */
    boolean processRequests() throws IOException, IgniteCheckedException {
        SessionChangeRequest poll = reqs.poll();

        if(poll != null) {
            do {
                poll.invoke(nio, this);
            }
            while ((poll = reqs.poll()) != null);

            return true;
        }

        return false;
    }

    /**
     * Processes read and write events and registration requests.
     *
     * @throws IgniteCheckedException If IOException occurred or thread was unable to add worker to workers pool.
     */
    private void bodyInternal() throws IgniteCheckedException {
        try {
            long lastIdleCheck = U.currentTimeMillis();

            while (!isCancelled && selector.isOpen()) {
                for (int i = 0; i < selectorSpins;) {
                    boolean processed = processRequests();
                    int keys = selector.selectNow();

                    if (keys > 0) {
                        // Walk through the ready keys collection and process network events.
                        if (selectedKeys == null)
                            processSelectedKeys(selector.selectedKeys());
                        else
                            processSelectedKeys(selectedKeys.flip());
                    }

                    lastIdleCheck = checkIdle(lastIdleCheck);

                    if (processed || keys > 0)
                        // Reset spin counter.
                        i = 0;
                    else
                        i++;
                }

                int keys;

                if(!processRequests()) {
                    selectFlag.set(true);

                    if(processRequests()) { // double check
                        selectFlag.set(false);

                        keys = selector.selectNow();
                    }
                    else{
                        keys = selector.select(SELECT_TIMEOUT);

                        if (keys == 0 && !selectFlag.get()) {
                            // somebody adds a request, possibly it's a write request
                            // we have to process requests and select once again
                            continue;
                        } else
                            selectFlag.set(false);
                    }
                }
                else
                    keys = selector.selectNow();

                if (keys > 0) {
                    // Walk through the ready keys collection and process network events.
                    if (selectedKeys == null)
                        processSelectedKeys(selector.selectedKeys());
                    else
                        processSelectedKeys(selectedKeys.flip());
                }

                lastIdleCheck = checkIdle(lastIdleCheck);
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
            closeSelector();
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

    /** */
    private void closeSelector() {
        if (selector.isOpen()) {
            if (log.isDebugEnabled())
                log.debug("Closing all connected client sockets.");

            // Close all channels registered with selector.
            for (SelectionKey key : selector.keys()) {
                GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

                if (attach != null && attach.hasSession())
                    closeSession(attach.session(), null);
            }

            if (log.isDebugEnabled())
                log.debug("Closing NIO selector.");

            U.close(selector, log);
        }
    }

    /**
     * @param sb Message builder.
     * @param keys Keys.
     */
    private void dumpSelectorInfo(StringBuilder sb, Collection<SelectionKey> keys) {
        sb.append(">> Selector info [idx=").append(idx)
            .append(", keysCnt=").append(keys.size())
            .append(", bytesRcvd=").append(bytesReceivedTotal())
            .append(", bytesRcvd0=").append(bytesReceivedSinceRebalancing())
            .append(", bytesSent=").append(bytesSentTotal())
            .append(", bytesSent0=").append(bytesSentSinceRebalancing())
            .append("]").append(U.nl());
    }

    /**
     * Processes keys selected by a selector.
     *
     * @param keys Selected keys.
     * @throws ClosedByInterruptException If this thread was interrupted while reading data.
     */
    private void processSelectedKeys(SelectionKey[] keys) throws ClosedByInterruptException {
        for (int i = 0; ; i++) {
            final SelectionKey key = keys[i];

            if (key == null)
                break;

            // null out entry in the array to allow to have it GC'ed once the Channel close
            // See https://github.com/netty/netty/issues/2363
            keys[i] = null;

            processSelectedKey(key);
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

            processSelectedKey(key);
        }
    }

    /**
     * Processes the given key.
     * @param key Selection key.
     * @throws ClosedByInterruptException If failed.
     */
    private void processSelectedKey(SelectionKey key) throws ClosedByInterruptException {
        // Was key closed?
        if (!key.isValid())
            return;

        GridNioKeyAttachment attach = (GridNioKeyAttachment)key.attachment();

        assert attach != null;

        try {
            if (!attach.hasSession() && key.isConnectable()) {
                processConnect(key);

                return;
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

            closeSession(ses, new GridNioException(e));
        }
    }

    /**
     * Checks sessions assigned to a selector for timeouts.
     * @param lastIdleCheck Last idle check time.
     * @return Last idle check time.
     */
    private long checkIdle(long lastIdleCheck) {
        long now = U.currentTimeMillis();

        if (now - lastIdleCheck > SELECT_TIMEOUT) {
            lastIdleCheck = now;

            for (SelectionKey key : selector.keys()) {
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
                    closeSession(ses, e);
                }
            }
        }

        return lastIdleCheck;
    }

    /**
     * @param fut Connection operation future.
     * @return Newly created session.
     * @throws IOException If failed.
     */
    private GridSelectorNioSessionImpl newSession(ConnectionOperationFuture fut) throws IOException {
        SocketChannel sockCh = fut.socketChannel();
        Socket sock = sockCh.socket();

        final GridSelectorNioSessionImpl ses = new GridSelectorNioSessionImpl(
            log,
            this,
            filterChain,
            (InetSocketAddress)sockCh.getLocalAddress(),
            (InetSocketAddress)sockCh.getRemoteAddress(),
            fut.accepted(),
            sndQueueLimit,
            directMode ? allocateBuffer(sock.getSendBufferSize()) : null,
            directMode ? allocateBuffer(sock.getReceiveBufferSize()) : null);

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
        return ses;
    }

    /**
     * @param key Key.
     * @throws IOException If failed.
     */
    private void processConnect(SelectionKey key) throws IOException {
        SocketChannel ch = (SocketChannel)key.channel();

        ConnectionOperationFuture sesFut = (ConnectionOperationFuture)key.attachment();

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
