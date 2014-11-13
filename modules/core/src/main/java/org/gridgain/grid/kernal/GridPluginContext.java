/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.design.plugin.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.direct.*;

import java.util.*;

/**
 *
 */
public class GridPluginContext implements PluginContext {
    /** */
    private final PluginConfiguration cfg;

    /** */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     * @param cfg Plugin configuration.
     */
    public GridPluginContext(GridKernalContext ctx, PluginConfiguration cfg) {
        this.cfg = cfg;
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public PluginConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public Grid grid() {
        return ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> nodes() {
        return ctx.discovery().allNodes();
    }

    /** {@inheritDoc} */
    @Override public GridNode localNode() {
        return ctx.discovery().localNode();
    }

    /** {@inheritDoc} */
    @Override public GridLogger log(Class<?> cls) {
        return ctx.log(cls);
    }

    /** {@inheritDoc} */
    @Override public void registerPort(int port, GridPortProtocol proto, Class<?> cls) {
        ctx.ports().registerPort(port, proto, cls);
    }

    /** {@inheritDoc} */
    @Override public void deregisterPort(int port, GridPortProtocol proto, Class<?> cls) {
        ctx.ports().deregisterPort(port, proto, cls);
    }

    /** {@inheritDoc} */
    @Override public void deregisterPorts(Class<?> cls) {
        ctx.ports().deregisterPorts(cls);
    }

    /** {@inheritDoc} */
    @Override public byte registerMessageProducer(GridTcpCommunicationMessageProducer producer) {
        return ctx.registerMessageProducer(producer);
    }
}
