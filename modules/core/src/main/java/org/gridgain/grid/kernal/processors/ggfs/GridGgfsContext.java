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

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * GGFS context holding all required components for GGFS instance.
 */
public class GridGgfsContext {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Configuration. */
    private final IgniteFsConfiguration cfg;

    /** Managers. */
    private List<GridGgfsManager> mgrs = new LinkedList<>();

    /** Meta manager. */
    private final GridGgfsMetaManager metaMgr;

    /** Data manager. */
    private final GridGgfsDataManager dataMgr;

    /** Server manager. */
    private final GridGgfsServerManager srvMgr;

    /** Fragmentizer manager. */
    private final GridGgfsFragmentizerManager fragmentizerMgr;

    /** GGFS instance. */
    private final GridGgfsEx ggfs;

    /**
     * @param ctx Kernal context.
     * @param cfg GGFS configuration.
     * @param metaMgr Meta manager.
     * @param dataMgr Data manager.
     * @param srvMgr Server manager.
     * @param fragmentizerMgr Fragmentizer manager.
     * @throws IgniteCheckedException If GGFs context instantiation is failed.
     */
    public GridGgfsContext(
        GridKernalContext ctx,
        IgniteFsConfiguration cfg,
        GridGgfsMetaManager metaMgr,
        GridGgfsDataManager dataMgr,
        GridGgfsServerManager srvMgr,
        GridGgfsFragmentizerManager fragmentizerMgr
    ) throws IgniteCheckedException {
        this.ctx = ctx;
        this.cfg = cfg;

        this.metaMgr = add(metaMgr);
        this.dataMgr = add(dataMgr);
        this.srvMgr = add(srvMgr);
        this.fragmentizerMgr = add(fragmentizerMgr);

        ggfs = new GridGgfsImpl(this);
    }

    /**
     * @return GGFS instance.
     */
    public GridGgfsEx ggfs() {
        return ggfs;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * @return GGFS configuration.
     */
    public IgniteFsConfiguration configuration() {
        return cfg;
    }

    /**
     * @return List of managers, in starting order.
     */
    public List<GridGgfsManager> managers() {
        return mgrs;
    }

    /**
     * @return Meta manager.
     */
    public GridGgfsMetaManager meta() {
        return metaMgr;
    }

    /**
     * @return Data manager.
     */
    public GridGgfsDataManager data() {
        return dataMgr;
    }

    /**
     * @return Server manager.
     */
    public GridGgfsServerManager server() {
        return srvMgr;
    }

    /**
     * @return Fragmentizer manager.
     */
    public GridGgfsFragmentizerManager fragmentizer() {
        return fragmentizerMgr;
    }

    /**
     * @param nodeId Node ID.
     * @param topic Topic.
     * @param msg Message.
     * @param plc Policy.
     * @throws IgniteCheckedException In case of error.
     */
    public void send(UUID nodeId, Object topic, GridGgfsCommunicationMessage msg, GridIoPolicy plc)
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
    public void send(ClusterNode node, Object topic, GridGgfsCommunicationMessage msg, GridIoPolicy plc)
        throws IgniteCheckedException {
        if (!kernalContext().localNodeId().equals(node.id()))
            msg.prepareMarshal(kernalContext().config().getMarshaller());

        kernalContext().io().send(node, topic, msg, plc);
    }

    /**
     * Checks if given node is a GGFS node.
     *
     * @param node Node to check.
     * @return {@code True} if node has GGFS with this name, {@code false} otherwise.
     */
    public boolean ggfsNode(ClusterNode node) {
        assert node != null;

        GridGgfsAttributes[] ggfs = node.attribute(ATTR_GGFS);

        if (ggfs != null)
            for (GridGgfsAttributes attrs : ggfs)
                if (F.eq(cfg.getName(), attrs.ggfsName()))
                    return true;

        return false;
    }

    /**
     * Adds manager to managers list.
     *
     * @param mgr Manager.
     * @return Added manager.
     */
    private <T extends GridGgfsManager> T add(@Nullable T mgr) {
        if (mgr != null)
            mgrs.add(mgr);

        return mgr;
    }
}
