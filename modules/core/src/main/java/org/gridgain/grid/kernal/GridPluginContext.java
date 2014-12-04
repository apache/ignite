/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.plugin.*;
import org.gridgain.grid.*;
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

    /** */
    private GridConfiguration igniteCfg;

    /**
     * @param ctx Kernal context.
     * @param cfg Plugin configuration.
     */
    public GridPluginContext(GridKernalContext ctx, PluginConfiguration cfg, GridConfiguration igniteCfg) {
        this.cfg = cfg;
        this.ctx = ctx;
        this.igniteCfg = igniteCfg;
    }

    /** {@inheritDoc} */
    @Override public <C extends PluginConfiguration> C configuration() {
        return (C)cfg;
    }

    /** {@inheritDoc} */
    @Override public GridConfiguration igniteConfiguration() {
        return igniteCfg;
    }

    /** {@inheritDoc} */
    @Override public Ignite grid() {
        return ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return ctx.discovery().allNodes();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
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
