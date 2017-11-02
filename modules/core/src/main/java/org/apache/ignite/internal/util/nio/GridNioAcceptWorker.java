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
import java.net.SocketAddress;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * A separate thread that will accept incoming connections and schedule read to some worker.
 */
class GridNioAcceptWorker<T> extends GridWorker {
    /** */
    private GridNioServer<T> nio;

    /** */
    private final SocketAddress addr;

    /** Selector for this thread. */
    private Selector selector;

    /** */
    private final IgniteRunnable balancer;

    /** Tcp no delay flag for newly accepted sockets. */
    private final boolean tcpNoDelay;

    /** Send buffer size  for newly accepted sockets. */
    private final int sockSndBuf;

    /** Receive buffer size  for newly accepted sockets. */
    private final int sockRcvBuf;

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param name Thread name.
     * @param balancer Balancer.
     * @param tcpNoDelay Tcp no delay flag for newly accepted sockets.
     * @param sockSndBuf Send buffer size  for newly accepted sockets.
     * @param sockRcvBuf Receive buffer size  for newly accepted sockets.
     * @param log Log.
     */
    GridNioAcceptWorker(GridNioServer<T> nio,
        String igniteInstanceName,
        String name,
        SocketAddress addr,
        Selector selector,
        IgniteRunnable balancer,
        boolean tcpNoDelay,
        int sockSndBuf,
        int sockRcvBuf,
        IgniteLogger log) throws IgniteCheckedException {
        super(igniteInstanceName, name, log);

        this.nio = nio;
        this.addr = addr;
        this.tcpNoDelay = tcpNoDelay;
        this.sockSndBuf = sockSndBuf;
        this.sockRcvBuf = sockRcvBuf;
        this.balancer = balancer;

        this.selector = selector;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        try {
            boolean reset = false;

            while (!nio.closed() && !Thread.currentThread().isInterrupted()) {
                try {
                    if (reset)
                        selector = nio.createSelector(addr);

                    accept();
                }
                catch (IgniteCheckedException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        U.error(log, "Failed to accept remote connection (will wait for " + GridNioServer.ERR_WAIT_TIME + "ms).",
                            e);

                        U.sleep(GridNioServer.ERR_WAIT_TIME);

                        reset = true;
                    }
                }
            }
        }
        finally {
            closeSelector(); // Safety.
        }
    }

    /**
     * Accepts connections and schedules them for processing by one of read workers.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void accept() throws IgniteCheckedException {
        try {
            while (!nio.closed() && selector.isOpen() && !Thread.currentThread().isInterrupted()) {
                // Wake up every 2 seconds to check if closed.
                if (selector.select(2000) > 0)
                    // Walk through the ready keys collection and process date requests.
                    processSelectedKeys(selector.selectedKeys());

                if (balancer != null)
                    balancer.run();
            }
        }
        // Ignore this exception as thread interruption is equal to 'close' call.
        catch (ClosedByInterruptException e) {
            if (log.isDebugEnabled())
                log.debug("Closing selector due to thread interruption [srvr=" + this +
                    ", err=" + e.getMessage() + ']');
        }
        catch (ClosedSelectorException e) {
            throw new IgniteCheckedException("Selector got closed while active: " + this, e);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to accept connection: " + this, e);
        }
        finally {
            closeSelector();
        }
    }

    /**
     * Close selector if needed.
     */
    private void closeSelector() {
        if (selector.isOpen()) {
            if (log.isDebugEnabled())
                log.debug("Closing all listening sockets.");

            // Close all channels registered with selector.
            for (SelectionKey key : selector.keys())
                U.close(key.channel(), log);

            if (log.isDebugEnabled())
                log.debug("Closing NIO selector.");

            U.close(selector, log);
        }
    }

    /**
     * Processes selected accept requests for server socket.
     *
     * @param keys Selected keys from acceptor.
     * @throws IOException If accept failed or IOException occurred while configuring channel.
     */
    private void processSelectedKeys(Set<SelectionKey> keys) throws IOException {
        if (log.isDebugEnabled())
            log.debug("Processing keys in accept worker: " + keys.size());

        for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext();) {
            SelectionKey key = iter.next();

            iter.remove();

            // Was key closed?
            if (!key.isValid())
                continue;

            if (key.isAcceptable()) {
                // The key indexes into the selector so we
                // can retrieve the socket that's ready for I/O
                ServerSocketChannel srvrCh = (ServerSocketChannel)key.channel();

                SocketChannel sockCh = srvrCh.accept();

                sockCh.configureBlocking(false);
                sockCh.socket().setTcpNoDelay(tcpNoDelay);
                sockCh.socket().setKeepAlive(true);

                if (sockSndBuf > 0)
                    sockCh.socket().setSendBufferSize(sockSndBuf);

                if (sockRcvBuf > 0)
                    sockCh.socket().setReceiveBufferSize(sockRcvBuf);

                if (log.isDebugEnabled())
                    log.debug("Accepted new client connection: " + sockCh.socket().getRemoteSocketAddress());

                addRegistrationRequest(sockCh);
            }
        }
    }

    /**
     * Adds registration request for a given socket channel to the next selector. Next selector
     * is selected according to a round-robin algorithm.
     *
     * @param sockCh Socket channel to be registered on one of the selectors.
     */
    private void addRegistrationRequest(SocketChannel sockCh) {
        nio.offerBalanced(new ConnectionOperationFuture(sockCh, true, null, NioOperation.REGISTER), null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioAcceptWorker.class, this, super.toString());
    }
}
