/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.util.typedef.*;
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
    private final GridGgfsConfiguration cfg;

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
     * @throws GridException If GGFs context instantiation is failed.
     */
    public GridGgfsContext(
        GridKernalContext ctx,
        GridGgfsConfiguration cfg,
        GridGgfsMetaManager metaMgr,
        GridGgfsDataManager dataMgr,
        GridGgfsServerManager srvMgr,
        GridGgfsFragmentizerManager fragmentizerMgr
    ) throws GridException {
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
    public GridGgfsConfiguration configuration() {
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
     * @throws GridException In case of error.
     */
    public void send(UUID nodeId, Object topic, GridGgfsCommunicationMessage msg, GridIoPolicy plc)
        throws GridException {
        if (!kernalContext().localNodeId().equals(nodeId))
            msg.prepareMarshal(kernalContext().config().getMarshaller());

        kernalContext().io().send(nodeId, topic, msg, plc);
    }

    /**
     * @param node Node.
     * @param topic Topic.
     * @param msg Message.
     * @param plc Policy.
     * @throws GridException In case of error.
     */
    public void send(ClusterNode node, Object topic, GridGgfsCommunicationMessage msg, GridIoPolicy plc)
        throws GridException {
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
