// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.segmentation.tcp;

import org.gridgain.grid.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Segmentation resolver implementation that checks whether
 * node is in the correct segment or not by establishing TCP
 * connection to configured host and port and immediately closing it.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridTcpSegmentationResolver implements GridSegmentationResolver {
    /** Default value for local port. */
    public static final int DFLT_LOC_PORT = 0;

    /** Default value for local port. */
    public static final int DFLT_CONN_TIMEOUT = 1000;

    /** Address to establish connection to. */
    private InetSocketAddress addr;

    /** Host to establish connection to. */
    private String host;

    /** Port to establish connection to. */
    private int port = -1;

    /** Local address. */
    private InetAddress locAddr;

    /** Local port. */
    private int locPort = DFLT_LOC_PORT;

    /** Connect timeout. */
    private int connTimeout = DFLT_CONN_TIMEOUT;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override public boolean isValidSegment() throws GridException {
        init();

        Socket sock = null;

        try {
            sock = new Socket();

            sock.bind(new InetSocketAddress(locAddr, locPort));

            sock.connect(new InetSocketAddress(addr.getAddress(), addr.getPort()), connTimeout);

            return true;
        }
        catch (IOException e) {
            throw new GridException("Failed to check address reachability: " + addr, e);
        }
        finally {
            U.closeQuiet(sock);
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
                if (addr == null) {
                    if (host == null || port < 0) {
                        throw new GridException("Failed to initialize address of the segmentation resolver " +
                            "(either address or host and port should be set): " + this);
                    }

                    addr = new InetSocketAddress(host, port);
                }

                if (locPort < 0)
                    throw new GridException("Failed to initialize segmentation resolver " +
                        "(local port cannot be negative): " + this);

                if (connTimeout < 0)
                    throw new GridException("Failed to initialize segmentation resolver " +
                        "(connect timeout cannot be negative): " + this);
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            U.await(initLatch);

            if (addr == null)
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
     * @see #setHost(String)
     * @see #setPort(int)
     */
    public void setAddress(InetSocketAddress addr) {
        this.addr = addr;
    }

    /**
     * Convenient way to set address.
     * <p>
     * This is required property. Address has to be set using one of the available methods.
     *
     * @param addr Address to check reachability of.
     * @throws GridException If host is unknown or failed to parse address.
     * @see #setAddress(InetSocketAddress)
     * @see #setHost(String)
     * @see #setPort(int)
     */
    public void setAddressAsString(String addr) throws GridException {
        A.notNull(addr, "addr");

        StringTokenizer st = new StringTokenizer(addr, ":");

        if (st.countTokens() != 2)
            throw new GridException("Failed to parse address");

        String addrStr = st.nextToken();
        String portStr = st.nextToken();

        try {
            int port = Integer.parseInt(portStr);

            this.addr = new InetSocketAddress(addrStr, port);
        }
        catch (IllegalArgumentException e) {
            throw new GridException("Failed to parse provided address: " + addr, e);
        }
    }

    /**
     * Convenient way to set address.
     * <p>
     * This is required property. Address has to be set using one of the available methods.
     *
     * @param host Host name to check reachability of.
     * @see #setAddress(InetSocketAddress)
     * @see #setAddressAsString(String)
     * @see #setPort(int)
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Convenient way to set address.
     * <p>
     * This is required property. Address has to be set using one of the available methods.
     *
     * @param port Port number on the host to check reachability of.
     * @see #setAddress(InetSocketAddress)
     * @see #setAddressAsString(String)
     * @see #setHost(String)
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Sets local address.
     * <p>
     * If not specified, check will be performed through any local address available.
     *
     * @param locAddr Local address.
     * @throws GridException If host is unknown.
     * @see #setLocalAddressAsString(String)
     */
    public void setLocalAddress(InetAddress locAddr) throws GridException {
        this.locAddr = locAddr;
    }

    /**
     * Convenient way to set local address.
     * <p>
     * If not specified, check will be performed through any local address available.
     *
     * @param locAddr Local address.
     * @throws GridException If host is unknown.
     * @see #setLocalAddress(InetAddress)
     */
    public void setLocalAddressAsString(String locAddr) throws GridException {
        try {
            this.locAddr = InetAddress.getByName(locAddr);
        }
        catch (UnknownHostException e) {
            throw new GridException("Failed to get address by name: " + addr, e);
        }
    }

    /**
     * Sets local port.
     * <p>
     * If not specified, {@link #DFLT_LOC_PORT} is used.
     *
     * @param locPort Local port.
     */
    public void setLocalPort(int locPort) {
        this.locPort = locPort;
    }

    /**
     * Gets connect timeout.
     *
     * @return Connect timeout.
     */
    public int getConnectTimeout() {
        return connTimeout;
    }

    /**
     * Sets connect timeout.
     * <p>
     * If not specified, {@link #DFLT_CONN_TIMEOUT} is used.
     *
     * @param connTimeout Connect timeout.
     */
    public void setConnectTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpSegmentationResolver.class, this);
    }
}
