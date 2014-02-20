// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.segmentation.reachability;

import org.gridgain.grid.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Segmentation resolver that uses address reachability to check whether node is in the
 * correct segment or not.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReachabilitySegmentationResolver implements GridSegmentationResolver {
    /** Default value for TTL. */
    public static final int DFLT_TTL = 0;

    /** Default value for timeout. */
    public static final int DFLT_TIMEOUT = 1000;

    /** Address to check reachability of. */
    private InetAddress addr;

    /** Interface address. */
    private InetAddress itfAddr;

    /** Interface. */
    private NetworkInterface itf;

    /** Timeout. */
    private int timeout = DFLT_TIMEOUT;

    /** TTL. */
    private int ttl = DFLT_TTL;

    /** Successful initialization flag. */
    private boolean init;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override public boolean isValidSegment() throws GridException {
        init();

        try {
            return addr.isReachable(itf, ttl, timeout);
        }
        catch (IOException e) {
            throw new GridException("Failed to check address reachability: " + addr, e);
        }
    }

    /**
     * Initializes checker.
     *
     * @throws GridException If failed.
     */
    private void init() throws GridException {
        if (initGuard.compareAndSet(false, true)) {
            try {
                if (addr == null)
                    throw new GridException("Property 'address' cannot be null: " + this);

                if (itf == null && itfAddr != null)
                    itf = NetworkInterface.getByInetAddress(itfAddr);

                if (ttl < 0)
                    throw new GridException("Property 'TTL' cannot be negative: " + this);

                if (timeout < 0)
                    throw new GridException("Property 'timeout' cannot be negative: " + this);

                init = true;
            }
            catch (SocketException e) {
                throw new GridException("Failed to get network interface: " + this, e);
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            U.await(initLatch);

            if (!init)
                throw new GridException("Segmentation resolver was not properly initialized.");
        }
    }

    /**
     * Sets target address to check reachability of.
     * <p>
     * This is required property. Address has to be set using one of the available methods.
     *
     * @param addr Address to check reachability of.
     * @see #setAddressAsString(String)
     */
    public void setAddress(InetAddress addr) {
        this.addr = addr;
    }

    /**
     * Convenient way to set address.
     * <p>
     * This is required property. Address has to be set using one of the available methods.
     *
     * @param addr Address to check reachability of.
     * @throws GridException If host is unknown.
     * @see #setAddress(InetAddress)
     */
    public void setAddressAsString(String addr) throws GridException {
        A.notNull(addr, "addr");

        try {
            this.addr = InetAddress.getByName(addr);
        }
        catch (UnknownHostException e) {
            throw new GridException("Failed to get address by name: " + addr, e);
        }
    }

    /**
     * Sets network interface address to check reachability through.
     * <p>
     * If interface is not specified, check will be performed through any interface available.
     *
     * @param itfAddr Network interface address.
     * @see #setNetworkInterfaceAddressAsString(String)
     * @see #setNetworkInterface(NetworkInterface)
     */
    public void setNetworkInterfaceAddress(InetAddress itfAddr) {
        this.itfAddr = itfAddr;
    }

    /**
     * Convenient way to set network interface address.
     * <p>
     * If interface is not specified, check will be performed through any interface available.
     *
     * @param itfAddr Network interface address.
     * @throws GridException If host is unknown.
     * @see #setNetworkInterfaceAddress(InetAddress)
     * @see #setNetworkInterface(NetworkInterface)
     */
    public void setNetworkInterfaceAddressAsString(String itfAddr) throws GridException {
        A.notNull(itfAddr, "itfAddr");

        try {
            this.itfAddr = InetAddress.getByName(itfAddr);
        }
        catch (UnknownHostException e) {
            throw new GridException("Failed to get address by name: " + addr, e);
        }
    }

    /**
     * In case address is bound to several interfaces this method should be used to
     * explicitly provide necessary interface.
     * <p>
     * If interface is not specified, check will be performed through any interface available.
     *
     * @param itf Network interface.
     * @see #setNetworkInterfaceAddress(InetAddress)
     * @see #setNetworkInterfaceAddressAsString(String)
     */
    public void setNetworkInterface(NetworkInterface itf) {
        this.itf = itf;
    }

    /**
     * Sets timeout to use when checking reachability.
     * <p>
     * If not provided, {@link #DFLT_TIMEOUT} is used.
     *
     * @param timeout Timeout.
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * Sets TTL to use when checking reachability.
     * <p>
     * If not provided, {@link #DFLT_TTL} is used.
     *
     * @param ttl TTL.
     */
    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReachabilitySegmentationResolver.class, this);
    }
}
