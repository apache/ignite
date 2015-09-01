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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGFS;

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
        assert node != null;

        IgfsAttributes[] igfs = node.attribute(ATTR_IGFS);

        if (igfs != null)
            for (IgfsAttributes attrs : igfs)
                if (F.eq(cfg.getName(), attrs.igfsName()))
                    return true;

        return false;
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