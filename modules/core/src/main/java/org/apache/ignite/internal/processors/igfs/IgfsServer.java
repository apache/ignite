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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.igfs.common.*;
import org.apache.ignite.internal.util.ipc.*;
import org.apache.ignite.internal.util.ipc.loopback.*;
import org.apache.ignite.internal.util.ipc.shmem.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.thread.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.apache.ignite.spi.IgnitePortProtocol.*;

/**
 * IGFS server. Handles requests passed from IGFS clients.
 */
public class IgfsServer {
    /** IGFS context. */
    private final IgfsContext igfsCtx;

    /** Logger. */
    private final IgniteLogger log;

    /** IGFS marshaller. */
    private final IgfsMarshaller marsh;

    /** Endpoint configuration. */
    private final IgfsIpcEndpointConfiguration endpointCfg;

    /** Server endpoint. */
    private IpcServerEndpoint srvEndpoint;

    /** Server message handler. */
    private IgfsServerHandler hnd;

    /** Accept worker. */
    private AcceptWorker acceptWorker;

    /** Started client workers. */
    private ConcurrentLinkedDeque8<ClientWorker> clientWorkers = new ConcurrentLinkedDeque8<>();

    /** Flag indicating if this a management endpoint. */
    private final boolean mgmt;

    /**
     * Constructs igfs server manager.
     * @param igfsCtx IGFS context.
     * @param endpointCfg Endpoint configuration to start.
     * @param mgmt Management flag - if true, server is intended to be started for Visor.
     */
    public IgfsServer(IgfsContext igfsCtx, IgfsIpcEndpointConfiguration endpointCfg, boolean mgmt) {
        assert igfsCtx != null;
        assert endpointCfg != null;

        this.endpointCfg = endpointCfg;
        this.igfsCtx = igfsCtx;
        this.mgmt = mgmt;

        log = igfsCtx.kernalContext().log(IgfsServer.class);

        marsh = new IgfsMarshaller();
    }

    /**
     * Starts this server.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void start() throws IgniteCheckedException {
        srvEndpoint = createEndpoint(endpointCfg, mgmt);

        if (U.isWindows() && srvEndpoint instanceof IpcSharedMemoryServerEndpoint)
            throw new IgniteCheckedException(IpcSharedMemoryServerEndpoint.class.getSimpleName() +
                " should not be configured on Windows (configure " +
                IpcServerTcpEndpoint.class.getSimpleName() + ")");

        if (srvEndpoint instanceof IpcServerTcpEndpoint) {
            IpcServerTcpEndpoint srvEndpoint0 = (IpcServerTcpEndpoint)srvEndpoint;

            srvEndpoint0.setManagement(mgmt);

            if (srvEndpoint0.getHost() == null) {
                if (mgmt) {
                    String locHostName = igfsCtx.kernalContext().config().getLocalHost();

                    try {
                        srvEndpoint0.setHost(U.resolveLocalHost(locHostName).getHostAddress());
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException("Failed to resolve local host: " + locHostName, e);
                    }
                }
                else
                    // Bind non-management endpoint to 127.0.0.1 by default.
                    srvEndpoint0.setHost("127.0.0.1");
            }
        }

        igfsCtx.kernalContext().resource().injectGeneric(srvEndpoint);

        srvEndpoint.start();

        // IpcServerEndpoint.getPort contract states return -1 if there is no port to be registered.
        if (srvEndpoint.getPort() >= 0)
            igfsCtx.kernalContext().ports().registerPort(srvEndpoint.getPort(), TCP, srvEndpoint.getClass());

        hnd = new IgfsIpcHandler(igfsCtx);

        // Start client accept worker.
        acceptWorker = new AcceptWorker();
    }

    /**
     * Create server IPC endpoint.
     *
     * @param endpointCfg Endpoint configuration.
     * @param mgmt Management flag.
     * @return Server endpoint.
     * @throws IgniteCheckedException If failed.
     */
    private static IpcServerEndpoint createEndpoint(IgfsIpcEndpointConfiguration endpointCfg, boolean mgmt)
        throws IgniteCheckedException {
        A.notNull(endpointCfg, "endpointCfg");

        IgfsIpcEndpointType typ = endpointCfg.getType();

        if (typ == null)
            throw new IgniteCheckedException("Failed to create server endpoint (type is not specified)");

        switch (typ) {
            case SHMEM: {
                IpcSharedMemoryServerEndpoint endpoint = new IpcSharedMemoryServerEndpoint();

                endpoint.setPort(endpointCfg.getPort());
                endpoint.setSize(endpointCfg.getMemorySize());
                endpoint.setTokenDirectoryPath(endpointCfg.getTokenDirectoryPath());

                return endpoint;
            }
            case TCP: {
                IpcServerTcpEndpoint endpoint = new IpcServerTcpEndpoint();

                endpoint.setHost(endpointCfg.getHost());
                endpoint.setPort(endpointCfg.getPort());
                endpoint.setManagement(mgmt);

                return endpoint;
            }
            default:
                throw new IgniteCheckedException("Failed to create server endpoint (type is unknown): " + typ);
        }
    }

    /**
     * Callback that is invoked when kernal is ready.
     */
    public void onKernalStart() {
        // Accept connections only when grid is ready.
        if (srvEndpoint != null)
            new IgniteThread(acceptWorker).start();
    }

    /**
     * Stops this server.
     *
     * @param cancel Cancel flag.
     */
    public void stop(boolean cancel) {
        // Skip if did not start.
        if (srvEndpoint == null)
            return;

        // Stop accepting new client connections.
        U.cancel(acceptWorker);

        U.join(acceptWorker, log);

        // Stop server handler, no more requests on existing connections will be processed.
        try {
            hnd.stop();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to stop IGFS server handler (will close client connections anyway).", e);
        }

        // Stop existing client connections.
        for (ClientWorker worker : clientWorkers)
            U.cancel(worker);

        U.join(clientWorkers, log);

        // IpcServerEndpoint.getPort contract states return -1 if there is no port to be registered.
        if (srvEndpoint.getPort() >= 0)
            igfsCtx.kernalContext().ports().deregisterPort(srvEndpoint.getPort(), TCP, srvEndpoint.getClass());

        try {
            igfsCtx.kernalContext().resource().cleanupGeneric(srvEndpoint);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to cleanup server endpoint.", e);
        }
    }

    /**
     * Gets IPC server endpoint.
     *
     * @return IPC server endpoint.
     */
    public IpcServerEndpoint getIpcServerEndpoint() {
        return srvEndpoint;
    }

    /**
     * Client reader thread.
     */
    private class ClientWorker extends GridWorker {
        /** Connected client endpoint. */
        private IpcEndpoint endpoint;

        /** Data output stream. */
        private final IgfsDataOutputStream out;

        /** Client session object. */
        private IgfsClientSession ses;

        /** Queue node for fast unlink. */
        private ConcurrentLinkedDeque8.Node<ClientWorker> node;

        /**
         * Creates client worker.
         *
         * @param idx Worker index for worker thread naming.
         * @param endpoint Connected client endpoint.
         * @throws IgniteCheckedException If endpoint output stream cannot be obtained.
         */
        protected ClientWorker(IpcEndpoint endpoint, int idx) throws IgniteCheckedException {
            super(igfsCtx.kernalContext().gridName(), "igfs-client-worker-" + idx, log);

            this.endpoint = endpoint;

            ses = new IgfsClientSession();

            out = new IgfsDataOutputStream(new BufferedOutputStream(endpoint.outputStream()));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                IgfsDataInputStream dis = new IgfsDataInputStream(endpoint.inputStream());

                byte[] hdr = new byte[IgfsMarshaller.HEADER_SIZE];

                boolean first = true;

                while (!Thread.currentThread().isInterrupted()) {
                    dis.readFully(hdr);

                    final long reqId = U.bytesToLong(hdr, 0);

                    int ordinal = U.bytesToInt(hdr, 8);

                    if (first) { // First message must be HANDSHAKE.
                        if (reqId != 0 || ordinal != IgfsIpcCommand.HANDSHAKE.ordinal()) {
                            if (log.isDebugEnabled())
                                log.debug("IGFS IPC handshake failed [reqId=" + reqId + ", ordinal=" + ordinal + ']');

                            return;
                        }

                        first = false;
                    }

                    final IgfsIpcCommand cmd = IgfsIpcCommand.valueOf(ordinal);

                    IgfsMessage msg = marsh.unmarshall(cmd, hdr, dis);

                    IgniteInternalFuture<IgfsMessage> fut = hnd.handleAsync(ses, msg, dis);

                    // If fut is null, no response is required.
                    if (fut != null) {
                        if (fut.isDone()) {
                            IgfsMessage res;

                            try {
                                res = fut.get();
                            }
                            catch (IgniteCheckedException e) {
                                res = new IgfsControlResponse();

                                ((IgfsControlResponse)res).error(e);
                            }

                            try {
                                synchronized (out) {
                                    // Reuse header.
                                    IgfsMarshaller.fillHeader(hdr, reqId, res.command());

                                    marsh.marshall(res, hdr, out);

                                    out.flush();
                                }
                            }
                            catch (IOException | IgniteCheckedException e) {
                                shutdown0(e);
                            }
                        }
                        else {
                            fut.listen(new CIX1<IgniteInternalFuture<IgfsMessage>>() {
                                @Override public void applyx(IgniteInternalFuture<IgfsMessage> fut) {
                                    IgfsMessage res;

                                    try {
                                        res = fut.get();
                                    }
                                    catch (IgniteCheckedException e) {
                                        res = new IgfsControlResponse();

                                        ((IgfsControlResponse)res).error(e);
                                    }

                                    try {
                                        synchronized (out) {
                                            byte[] hdr = IgfsMarshaller.createHeader(reqId, res.command());

                                            marsh.marshall(res, hdr, out);

                                            out.flush();
                                        }
                                    }
                                    catch (IOException | IgniteCheckedException e) {
                                        shutdown0(e);
                                    }
                                }
                            });
                        }
                    }
                }
            }
            catch (EOFException ignored) {
                // Client closed connection.
            }
            catch (IgniteCheckedException | IOException e) {
                if (!isCancelled())
                    U.error(log, "Failed to read data from client (will close connection)", e);
            }
            finally {
                onFinished();
            }
        }

        /**
         * @param node Node in queue for this worker.
         */
        public void node(ConcurrentLinkedDeque8.Node<ClientWorker> node) {
            this.node = node;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            super.cancel();

            shutdown0(null);
        }

        /**
         * @param e Optional exception occurred while stopping this
         */
        private void shutdown0(@Nullable Throwable e) {
            if (!isCancelled()) {
                if (e != null)
                    U.error(log, "Stopping client reader due to exception: " + endpoint, e);
            }

            U.closeQuiet(out);

            endpoint.close();
        }

        /**
         * Final resource cleanup.
         */
        private void onFinished() {
            // Second close is no-op, if closed manually.
            U.closeQuiet(out);

            endpoint.close();

            // Finally, remove from queue.
            if (clientWorkers.unlinkx(node))
                hnd.onClosed(ses);
        }
    }

    /**
     * Accept worker.
     */
    private class AcceptWorker extends GridWorker {
        /** Accept index. */
        private int acceptCnt;

        /**
         * Creates accept worker.
         */
        protected AcceptWorker() {
            super(igfsCtx.kernalContext().gridName(), "igfs-accept-worker", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    IpcEndpoint client = srvEndpoint.accept();

                    if (log.isDebugEnabled())
                        log.debug("IGFS client connected [igfsName=" + igfsCtx.kernalContext().gridName() +
                            ", client=" + client + ']');

                    ClientWorker worker = new ClientWorker(client, acceptCnt++);

                    IgniteThread workerThread = new IgniteThread(worker);

                    ConcurrentLinkedDeque8.Node<ClientWorker> node = clientWorkers.addx(worker);

                    worker.node(node);

                    workerThread.start();
                }
            }
            catch (IgniteCheckedException e) {
                if (!isCancelled())
                    U.error(log, "Failed to accept client IPC connection (will shutdown accept thread).", e);
            }
            finally {
                srvEndpoint.close();
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            super.cancel();

            srvEndpoint.close();
        }
    }
}
