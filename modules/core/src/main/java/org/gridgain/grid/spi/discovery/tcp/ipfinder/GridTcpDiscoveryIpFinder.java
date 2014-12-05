/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.ipfinder;

import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.*;

import java.net.*;
import java.util.*;

/**
 * IP finder interface for {@link GridTcpDiscoverySpi}.
 */
public interface GridTcpDiscoveryIpFinder {
    /**
     * Callback invoked when SPI context is initialized after {@link GridTcpDiscoverySpi#spiStart(String)}
     * method is completed, SPI context can be stored for future access.
     *
     * @param spiCtx Spi context.
     * @throws org.gridgain.grid.spi.IgniteSpiException In case of error.
     */
    public void onSpiContextInitialized(GridSpiContext spiCtx) throws IgniteSpiException;

    /**
     * Callback invoked prior to stopping grid before SPI context is destroyed.
     * Note that invoking SPI context after this callback is complete is considered
     * illegal and may produce unknown results.
     */
    public void onSpiContextDestroyed();

    /**
     * Initializes addresses discovery SPI binds to.
     *
     * @param addrs Addresses discovery SPI binds to.
     * @throws org.gridgain.grid.spi.IgniteSpiException In case of error.
     */
    public void initializeLocalAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException;

    /**
     * Gets all addresses registered in this finder.
     *
     * @return All known addresses, potentially empty, but never {@code null}.
     * @throws org.gridgain.grid.spi.IgniteSpiException In case of error.
     */
    public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException;

    /**
     * Checks whether IP finder is shared or not.
     * <p>
     * If it is shared then only coordinator can unregister addresses.
     * <p>
     * All nodes should register their address themselves, as early as possible on node start.
     *
     * @return {@code true} if IP finder is shared.
     */
    public boolean isShared();

    /**
     * Registers new addresses.
     * <p>
     * Implementation should accept duplicates quietly, but should not register address if it
     * is already registered.
     *
     * @param addrs Addresses to register. Not {@code null} and not empty.
     * @throws org.gridgain.grid.spi.IgniteSpiException In case of error.
     */
    public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException;

    /**
     * Unregisters provided addresses.
     * <p>
     * Implementation should accept addresses that are currently not
     * registered quietly (just no-op).
     *
     * @param addrs Addresses to unregister. Not {@code null} and not empty.
     * @throws org.gridgain.grid.spi.IgniteSpiException In case of error.
     */
    public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException;

    /**
     * Closes this IP finder and releases any system resources associated with it.
     */
    public void close();
}
