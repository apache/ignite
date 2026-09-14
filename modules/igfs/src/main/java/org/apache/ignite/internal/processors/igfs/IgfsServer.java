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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.igfs.common.*;
import org.apache.ignite.internal.thread.pool.IgniteThreadPoolExecutor;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.ipc.loopback.IpcServerTcpEndpoint;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;

import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.ignite.spi.IgnitePortProtocol.TCP;

/**
 * IGFS server. Handles requests passed from IGFS clients.
 */
public class IgfsServer {
    /** IGFS context. */
    private final IgfsContext igfsCtx;
    private final ConcurrentMap<String, IgfsContext> igfsCtxs;
    /** Logger. */
    private final IgniteLogger log;

    /** IGFS marshaller. */
    private final IgfsMarshaller marsh;

    /** Endpoint configuration. */
    private final IgfsIpcEndpointConfiguration endpointCfg;

    /** Server endpoint. */
    private IpcServerEndpoint srvEndpoint;

    /** Accept worker. */
    private AcceptWorker acceptWorker;

    /** Started client workers. */
    private ConcurrentLinkedDeque8<ClientWorker> clientWorkers = new ConcurrentLinkedDeque8<>();

    /** Flag indicating if this a management endpoint. */
    private final boolean mgmt;

    private IgniteThreadPoolExecutor pool;

    /**
     * Constructs igfs server manager.
     * @param igfsCtxs IGFS context.
     * @param endpointCfg Endpoint configuration to start.
     * @param mgmt Management flag - if true, server is intended to be started for Visor.
     */
    public IgfsServer(IgfsContext igfsCtx, ConcurrentMap<String, IgfsContext> igfsCtxs, IgfsIpcEndpointConfiguration endpointCfg, boolean mgmt) {
        assert igfsCtxs != null;
        assert endpointCfg != null;

        this.endpointCfg = endpointCfg;
        this.igfsCtx = igfsCtx;
        this.igfsCtxs = igfsCtxs;
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
        if (srvEndpoint.getPort() >= 0) {
            igfsCtx.kernalContext().ports().registerPort(srvEndpoint.getPort(), TCP, srvEndpoint.getClass());
            igfsCtx.kernalContext().addNodeAttribute("igfs.IpcServerEndpoint.port",srvEndpoint.getPort());
        }

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
    private IpcServerEndpoint createEndpoint(IgfsIpcEndpointConfiguration endpointCfg, boolean mgmt)
        throws IgniteCheckedException {
        A.notNull(endpointCfg, "endpointCfg");

        IgfsIpcEndpointType typ = endpointCfg.getType();

        if (typ == null)
            throw new IgniteCheckedException("Failed to create server endpoint (type is not specified)");

        switch (typ) {            
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
        // Create thread pool for request handling.
        int threadCnt = endpointCfg.getThreadCount();

        String prefix = "igfs-" + (mgmt ? "mgmt-" : "") + "-ipc";

        pool = new IgniteThreadPoolExecutor(prefix, igfsCtx.kernalContext().igniteInstanceName(), threadCnt, threadCnt,
                Long.MAX_VALUE, new LinkedBlockingQueue<Runnable>());

        // Accept connections only when grid is ready.
        if (srvEndpoint != null)
            new IgniteThread(igfsCtx.kernalContext().igniteInstanceName(),"IgfsServer", acceptWorker).start();
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

        U.shutdownNow(getClass(), pool, log);

        // Stop server handler, no more requests on existing connections will be processed.
        try {
            for(IgfsContext fsctx : igfsCtxs.values()){
                if(fsctx.hnd!=null) {
                    fsctx.hnd.stop();
                    fsctx.hnd = null;
                }
            }

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
        private final IpcEndpoint endpoint;

        /** Data output stream. */
        private final IgfsDataOutputStream out;

        /** Client session object. */
        private final IgfsClientSession ses;

        /** Queue node for fast unlink. */
        private ConcurrentLinkedDeque8.Node<ClientWorker> node;

        private IgfsContext currentIgfsCtx = null;

        /**
         * Creates client worker.
         *
         * @param idx Worker index for worker thread naming.
         * @param endpoint Connected client endpoint.
         * @throws IgniteCheckedException If endpoint output stream cannot be obtained.
         */
        protected ClientWorker(IpcEndpoint endpoint, int idx) throws IgniteCheckedException {
            super(igfsCtx.kernalContext().igniteInstanceName(), "igfs-client-worker-" + idx, IgfsServer.this.log);

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
                    if(cmd == IgfsIpcCommand.HANDSHAKE){
                        IgfsHandshakeRequest hands = (IgfsHandshakeRequest)msg;
                        currentIgfsCtx = igfsCtxs.get(hands.igfsName());
                        if(currentIgfsCtx==null){
                            log.warning("IGFS IPC handshake failed! igfs " + hands.igfsName() + " not find in " + igfsCtx.kernalContext().igniteInstanceName());
                            return;
                        }
                        if(currentIgfsCtx.hnd==null)
                            currentIgfsCtx.hnd = new IgfsIpcHandler(currentIgfsCtx, endpointCfg, mgmt, pool);
                    }

                    IgniteInternalFuture<IgfsMessage> fut = currentIgfsCtx.hnd.handleAsync(ses, msg, dis);

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
                currentIgfsCtx.hnd.onClosed(ses);
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
            super(igfsCtx.kernalContext().igniteInstanceName(), "igfs-accept-worker", IgfsServer.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    IpcEndpoint client = srvEndpoint.accept();

                    if (log.isDebugEnabled())
                        log.debug("IGFS client connected [igfsName=" + igfsCtx.kernalContext().igniteInstanceName() +
                            ", client=" + client + ']');

                    ClientWorker worker = new ClientWorker(client, acceptCnt++);

                    IgniteThread workerThread = new IgniteThread(igfsCtx.kernalContext().igniteInstanceName(),"IgfsServerWork", worker);

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
