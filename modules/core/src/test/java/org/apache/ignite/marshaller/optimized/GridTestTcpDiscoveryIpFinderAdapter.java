/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.marshaller.optimized;

import org.gridgain.grid.spi.IgniteSpiException;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.GridTcpDiscoveryIpFinderAdapter;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Test TCP discovery IP finder adapter.
 */
public class GridTestTcpDiscoveryIpFinderAdapter extends GridTcpDiscoveryIpFinderAdapter {
    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op.
    }
}
