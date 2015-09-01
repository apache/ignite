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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.internal.util.ipc.IpcEndpointBindException;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.igfs.IgfsIpcEndpointType.TCP;

/**
 * IGFS server manager.
 */
public class IgfsServerManager extends IgfsManager {
    /** IPC server rebind interval. */
    private static final long REBIND_INTERVAL = 3000;

    /** Collection of servers to maintain. */
    private Collection<IgfsServer> srvrs;

    /** Server port binders. */
    private BindWorker bindWorker;

    /** Kernal start latch. */
    private CountDownLatch kernalStartLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        FileSystemConfiguration igfsCfg = igfsCtx.configuration();

        if (igfsCfg.isIpcEndpointEnabled()) {
            IgfsIpcEndpointConfiguration ipcCfg = igfsCfg.getIpcEndpointConfiguration();

            if (ipcCfg == null)
                ipcCfg = new IgfsIpcEndpointConfiguration();

            bind(ipcCfg, /*management*/false);
        }

        if (igfsCfg.getManagementPort() >= 0) {
            IgfsIpcEndpointConfiguration mgmtIpcCfg = new IgfsIpcEndpointConfiguration();

            mgmtIpcCfg.setType(TCP);
            mgmtIpcCfg.setPort(igfsCfg.getManagementPort());

            bind(mgmtIpcCfg, /*management*/true);
        }

        if (bindWorker != null)
            new IgniteThread(bindWorker).start();
    }

    /**
     * Tries to start server endpoint with specified configuration. If failed, will print warning and start a thread
     * that will try to periodically start this endpoint.
     *
     * @param endpointCfg Endpoint configuration to start.
     * @param mgmt {@code True} if endpoint is management.
     * @throws IgniteCheckedException If failed.
     */
    private void bind(final IgfsIpcEndpointConfiguration endpointCfg, final boolean mgmt) throws IgniteCheckedException {
        if (srvrs == null)
            srvrs = new ConcurrentLinkedQueue<>();

        IgfsServer ipcSrv = new IgfsServer(igfsCtx, endpointCfg, mgmt);

        try {
            ipcSrv.start();

            srvrs.add(ipcSrv);
        }
        catch (IpcEndpointBindException ignored) {
            int port = ipcSrv.getIpcServerEndpoint().getPort();

            String portMsg = port != -1 ? " Failed to bind to port (is port already in use?): " + port : "";

            U.warn(log, "Failed to start IGFS " + (mgmt ? "management " : "") + "endpoint " +
                "(will retry every " + (REBIND_INTERVAL / 1000) + "s)." +
                portMsg);

            if (bindWorker == null)
                bindWorker = new BindWorker();

            bindWorker.addConfiguration(endpointCfg, mgmt);
        }
    }

    /**
     * @return Collection of active endpoints.
     */
    public Collection<IpcServerEndpoint> endpoints() {
        return F.viewReadOnly(srvrs, new C1<IgfsServer, IpcServerEndpoint>() {
            @Override public IpcServerEndpoint apply(IgfsServer e) {
                return e.getIpcServerEndpoint();
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (!F.isEmpty(srvrs)) {
            for (IgfsServer srv : srvrs)
                srv.onKernalStart();
        }

        kernalStartLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        // Safety.
        kernalStartLatch.countDown();

        if (bindWorker != null) {
            bindWorker.cancel();

            U.join(bindWorker, log);
        }

        if (!F.isEmpty(srvrs)) {
            for (IgfsServer srv : srvrs)
                srv.stop(cancel);
        }
    }

    /**
     * Bind worker.
     */
    @SuppressWarnings("BusyWait")
    private class BindWorker extends GridWorker {
        /** Configurations to bind. */
        private Collection<IgniteBiTuple<IgfsIpcEndpointConfiguration, Boolean>> bindCfgs = new LinkedList<>();

        /**
         * Constructor.
         */
        private BindWorker() {
            super(igfsCtx.kernalContext().gridName(), "bind-worker", igfsCtx.kernalContext().log());
        }

        /**
         * Adds configuration to bind on. Should not be called after thread start.
         *
         * @param cfg Configuration.
         * @param mgmt Management flag.
         */
        public void addConfiguration(IgfsIpcEndpointConfiguration cfg, boolean mgmt) {
            bindCfgs.add(F.t(cfg, mgmt));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            kernalStartLatch.await();

            while (!isCancelled()) {
                Thread.sleep(REBIND_INTERVAL);

                Iterator<IgniteBiTuple<IgfsIpcEndpointConfiguration, Boolean>> it = bindCfgs.iterator();

                while (it.hasNext()) {
                    IgniteBiTuple<IgfsIpcEndpointConfiguration, Boolean> cfg = it.next();

                    IgfsServer ipcSrv = new IgfsServer(igfsCtx, cfg.get1(), cfg.get2());

                    try {
                        ipcSrv.start();

                        ipcSrv.onKernalStart();

                        srvrs.add(ipcSrv);

                        it.remove();
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to bind IGFS endpoint [cfg=" + cfg + ", err=" + e.getMessage() + ']');
                    }
                }

                if (bindCfgs.isEmpty())
                    break;
            }
        }
    }
}