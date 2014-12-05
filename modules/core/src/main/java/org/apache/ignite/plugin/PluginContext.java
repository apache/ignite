// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.util.direct.*;

import java.util.*;

/**
 * TODO: Add class description.
 */
public interface PluginContext {
    /**
     * @return Plugin configuration.
     */
    public <C extends PluginConfiguration> C configuration();

    /**
     * @return Ignite configuration.
     */
    public IgniteConfiguration igniteConfiguration();

    /**
     * @return Grid.
     */
    public Ignite grid();

    /**
     * Gets a collection of all grid nodes. Remote nodes are discovered via underlying
     * {@link org.apache.ignite.spi.discovery.DiscoverySpi} implementation used.
     *
     * @return Collection of grid nodes.
     * @see #localNode()
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public Collection<ClusterNode> nodes();

    /**
     * Gets local grid node. Instance of local node is provided by underlying {@link org.apache.ignite.spi.discovery.DiscoverySpi}
     * implementation used.
     *
     * @return Local grid node.
     * @see org.apache.ignite.spi.discovery.DiscoverySpi
     */
    public ClusterNode localNode();

    /**
     * Gets logger for given class.
     *
     * @param cls Class to get logger for.
     * @return Logger.
     */
    public IgniteLogger log(Class<?> cls);

    /**
     * Registers open port.
     *
     * @param port Port.
     * @param proto Protocol.
     * @param cls Class.
     */
    public void registerPort(int port, IgnitePortProtocol proto, Class<?> cls);

    /**
     * Deregisters closed port.
     *
     * @param port Port.
     * @param proto Protocol.
     * @param cls Class.
     */
    public void deregisterPort(int port, IgnitePortProtocol proto, Class<?> cls);

    /**
     * Deregisters all closed ports.
     *
     * @param cls Class.
     */
    public void deregisterPorts(Class<?> cls);

    /**
     * @param producer Message producer.
     * @return Message type code.
     */
    public byte registerMessageProducer(GridTcpCommunicationMessageProducer producer);
}
