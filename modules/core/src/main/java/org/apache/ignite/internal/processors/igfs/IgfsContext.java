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

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS context holding all required components for IGFS instance.
 */
public class IgfsContext {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Configuration. */
    private final FileSystemConfiguration cfg;

    /** Managers. */
    private List<IgfsManager> mgrs = new LinkedList<>();

    /** Meta manager. */
    private final IgfsMetaManager metaMgr;

    /** Data manager. */
    private final IgfsDataManager dataMgr;

    /** Server manager. */
    private final IgfsServerManager srvMgr;

    /** Fragmentizer manager. */
    private final IgfsFragmentizerManager fragmentizerMgr;

    /** IGFS instance. */
    private final IgfsEx igfs;

    /** Local metrics holder. */
    private final IgfsLocalMetrics metrics = new IgfsLocalMetrics();

    /** Local cluster node. */
    private volatile ClusterNode locNode;

    /** IGFS executor service. */
    private ExecutorService igfsSvc;

    /** Logger. */
    protected IgniteLogger log;

    /**
     * @param ctx Kernal context.
     * @param cfg IGFS configuration.
     * @param metaMgr Meta manager.
     * @param dataMgr Data manager.
     * @param srvMgr Server manager.
     * @param fragmentizerMgr Fragmentizer manager.
     * @throws IgniteCheckedException If IGFS context instantiation is failed.
     */
    public IgfsContext(
        GridKernalContext ctx,
        FileSystemConfiguration cfg,
        IgfsMetaManager metaMgr,
        IgfsDataManager dataMgr,
        IgfsServerManager srvMgr,
        IgfsFragmentizerManager fragmentizerMgr
    ) throws IgniteCheckedException {
        this.ctx = ctx;
        this.cfg = cfg;

        this.metaMgr = add(metaMgr);
        this.dataMgr = add(dataMgr);
        this.srvMgr = add(srvMgr);
        this.fragmentizerMgr = add(fragmentizerMgr);

        log = ctx.log(IgfsContext.class);

        igfsSvc = ctx.getIgfsExecutorService();

        igfs = new IgfsImpl(this);
    }

    /**
     * @return IGFS instance.
     */
    public IgfsEx igfs() {
        return igfs;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * @return IGFS configuration.
     */
    public FileSystemConfiguration configuration() {
        return cfg;
    }

    /**
     * @return List of managers, in starting order.
     */
    public List<IgfsManager> managers() {
        return mgrs;
    }

    /**
     * @return Meta manager.
     */
    public IgfsMetaManager meta() {
        return metaMgr;
    }

    /**
     * @return Data manager.
     */
    public IgfsDataManager data() {
        return dataMgr;
    }

    /**
     * @return Server manager.
     */
    public IgfsServerManager server() {
        return srvMgr;
    }

    /**
     * @return Fragmentizer manager.
     */
    public IgfsFragmentizerManager fragmentizer() {
        return fragmentizerMgr;
    }

    /**
     * @param nodeId Node ID.
     * @param topic Topic.
     * @param msg Message.
     * @param plc Policy.
     * @throws IgniteCheckedException In case of error.
     */
    public void send(UUID nodeId, Object topic, IgfsCommunicationMessage msg, byte plc)
        throws IgniteCheckedException {
        if (!kernalContext().localNodeId().equals(nodeId))
            msg.prepareMarshal(kernalContext().config().getMarshaller());

        kernalContext().io().send(nodeId, topic, msg, plc);
    }

    /**
     * @param node Node.
     * @param topic Topic.
     * @param msg Message.
     * @param plc Policy.
     * @throws IgniteCheckedException In case of error.
     */
    public void send(ClusterNode node, Object topic, IgfsCommunicationMessage msg, byte plc)
        throws IgniteCheckedException {
        if (!kernalContext().localNodeId().equals(node.id()))
            msg.prepareMarshal(kernalContext().config().getMarshaller());

        kernalContext().io().send(node, topic, msg, plc);
    }

    /**
     * Checks if given node is a IGFS node.
     *
     * @param node Node to check.
     * @return {@code True} if node has IGFS with this name, {@code false} otherwise.
     */
    public boolean igfsNode(ClusterNode node) {
        return IgfsUtils.isIgfsNode(node, cfg.getName());
    }

    /**
     * Get local metrics.
     *
     * @return Local metrics.
     */
    public IgfsLocalMetrics metrics() {
        return metrics;
    }

    /**
     * Get local node.
     *
     * @return Local node.
     */
    public ClusterNode localNode() {
        if (locNode == null)
            locNode = ctx.discovery().localNode();

        return locNode;
    }

    /**
     * Executes runnable in IGFS executor service. If execution rejected, runnable will be executed
     * in caller thread.
     *
     * @param r Runnable to execute.
     */
    public void runInIgfsThreadPool(Runnable r) {
        try {
            igfsSvc.execute(r);
        }
        catch (RejectedExecutionException ignored) {
            // This exception will happen if network speed is too low and data comes faster
            // than we can send it to remote nodes.
            try {
                r.run();
            }
            catch (Exception e) {
                log.warning("Failed to execute IGFS runnable: " + r, e);
            }
        }
    }

    /**
     * Adds manager to managers list.
     *
     * @param mgr Manager.
     * @return Added manager.
     */
    private <T extends IgfsManager> T add(@Nullable T mgr) {
        if (mgr != null)
            mgrs.add(mgr);

        return mgr;
    }
}
