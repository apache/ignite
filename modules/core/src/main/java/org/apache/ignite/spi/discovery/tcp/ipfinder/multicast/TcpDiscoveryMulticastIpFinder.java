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

package org.apache.ignite.spi.discovery.tcp.ipfinder.multicast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiThread;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;
import static org.apache.ignite.spi.IgnitePortProtocol.UDP;

/**
 * Multicast-based IP finder.
 * <p>
 * When TCP discovery starts this finder sends multicast request and waits
 * for some time when others nodes reply to this request with messages containing
 * their addresses (time IP finder waits for response and number of attempts to
 * re-send multicast request in case if no replies are received can be configured,
 * see {@link #setResponseWaitTime(int)} and {@link #setAddressRequestAttempts(int)}).
 * <p>
 * In addition to address received via multicast this finder can work with pre-configured
 * list of addresses specified via {@link #setAddresses(Collection)} method.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Multicast IP address (see {@link #setMulticastGroup(String)}).</li>
 *      <li>Multicast port number (see {@link #setMulticastPort(int)}).</li>
 *      <li>Address response wait time (see {@link #setResponseWaitTime(int)}).</li>
 *      <li>Address request attempts (see {@link #setAddressRequestAttempts(int)}).</li>
 *      <li>Pre-configured addresses (see {@link #setAddresses(Collection)})</li>
 *      <li>Local address (see {@link #setLocalAddress(String)})</li>
 * </ul>
 */
public class TcpDiscoveryMulticastIpFinder extends TcpDiscoveryVmIpFinder {
    /** Default multicast IP address (value is {@code 228.1.2.4}). */
    public static final String DFLT_MCAST_GROUP = "228.1.2.4";

    /** Default multicast port number (value is {@code 47400}). */
    public static final int DFLT_MCAST_PORT = 47400;

    /** Default time IP finder waits for reply to multicast address request (value is {@code 500}). */
    public static final int DFLT_RES_WAIT_TIME = 500;

    /** Default number of attempts to send multicast address request (value is {@code 2}). */
    public static final int DFLT_ADDR_REQ_ATTEMPTS = 2;

    /** Address request message data. */
    private static final byte[] MSG_ADDR_REQ_DATA = U.IGNITE_HEADER;

    /** */
    private static final Marshaller marsh = new JdkMarshaller();

    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Multicast IP address as string. */
    private String mcastGrp = DFLT_MCAST_GROUP;

    /** Multicast port number. */
    private int mcastPort = DFLT_MCAST_PORT;

    /** Time IP finder waits for reply to multicast address request. */
    private int resWaitTime = DFLT_RES_WAIT_TIME;

    /** Number of attempts to send multicast address request. */
    private int addrReqAttempts = DFLT_ADDR_REQ_ATTEMPTS;

    /** Local address */
    private String locAddr;

    /** Time to live. */
    private Integer ttl;

    /** */
    @GridToStringExclude
    private Collection<AddressSender> addrSnds;

    /**
     * Constructs new IP finder.
     */
    public TcpDiscoveryMulticastIpFinder() {
        setShared(true);
    }

    /**
     * Sets IP address of multicast group.
     * <p>
     * If not provided, default value is {@link #DFLT_MCAST_GROUP}.
     *
     * @param mcastGrp Multicast IP address.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMulticastGroup(String mcastGrp) {
        this.mcastGrp = mcastGrp;
    }

    /**
     * Gets IP address of multicast group.
     *
     * @return Multicast IP address.
     */
    public String getMulticastGroup() {
        return mcastGrp;
    }

    /**
     * Sets port number which multicast messages are sent to.
     * <p>
     * If not provided, default value is {@link #DFLT_MCAST_PORT}.
     *
     * @param mcastPort Multicast port number.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setMulticastPort(int mcastPort) {
        this.mcastPort = mcastPort;
    }

    /**
     * Gets port number which multicast messages are sent to.
     *
     * @return Port number.
     */
    public int getMulticastPort() {
        return mcastPort;
    }

    /**
     * Sets time in milliseconds IP finder waits for reply to
     * multicast address request.
     * <p>
     * If not provided, default value is {@link #DFLT_RES_WAIT_TIME}.
     *
     * @param resWaitTime Time IP finder waits for reply to multicast address request.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setResponseWaitTime(int resWaitTime) {
        this.resWaitTime = resWaitTime;
    }

    /**
     * Gets time in milliseconds IP finder waits for reply to
     * multicast address request.
     *
     * @return Time IP finder waits for reply to multicast address request.
     */
    public int getResponseWaitTime() {
        return resWaitTime;
    }

    /**
     * Sets number of attempts to send multicast address request. IP finder re-sends
     * request only in case if no reply for previous request is received.
     * <p>
     * If not provided, default value is {@link #DFLT_ADDR_REQ_ATTEMPTS}.
     *
     * @param addrReqAttempts Number of attempts to send multicast address request.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setAddressRequestAttempts(int addrReqAttempts) {
        this.addrReqAttempts = addrReqAttempts;
    }

    /**
     * Gets number of attempts to send multicast address request. IP finder re-sends
     * request only in case if no reply for previous request is received.
     *
     * @return Number of attempts to send multicast address request.
     */
    public int getAddressRequestAttempts() {
        return addrReqAttempts;
    }

    /**
     * Sets local host address used by this IP finder. If provided address is non-loopback then multicast
     * socket is bound to this interface. If local address is not set or is any local address then IP finder
     * creates multicast sockets for all found non-loopback addresses.
     * <p>
     * If not provided then this property is initialized by the local address set in {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}
     * configuration.
     *
     * @param locAddr Local host address.
     * @see org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi#setLocalAddress(String)
     */
    @IgniteSpiConfiguration(optional = true)
    public void setLocalAddress(String locAddr) {
        this.locAddr = locAddr;
    }

    /**
     * Gets local address that multicast IP finder uses.
     *
     * @return Local address.
     */
    public String getLocalAddress() {
        return locAddr;
    }

    /**
     * Set the default time-to-live for multicast packets sent out on this
     * IP finder in order to control the scope of the multicast.
     * <p>
     * The TTL has to be in the range {@code  0 <= TTL <= 255}.
     * <p>
     * If TTL is {@code 0}, packets are not transmitted on the network,
     * but may be delivered locally.
     *
     * @param ttl Time to live.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setTimeToLive(int ttl) {
        this.ttl = ttl;
    }

    /**
     * Set the default time-to-live for multicast packets sent out on this
     * IP finder.
     *
     * @return Time to live.
     */
    public int getTimeToLive() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public void initializeLocalAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // If IGNITE_OVERRIDE_MCAST_GRP system property is set, use its value to override multicast group from
        // configuration. Used for testing purposes.
        String overrideMcastGrp = System.getProperty(IGNITE_OVERRIDE_MCAST_GRP);

        if (overrideMcastGrp != null)
            mcastGrp = overrideMcastGrp;

        if (F.isEmpty(mcastGrp))
            throw new IgniteSpiException("Multicast IP address is not specified.");

        if (mcastPort < 0 || mcastPort > 65535)
            throw new IgniteSpiException("Invalid multicast port: " + mcastPort);

        if (resWaitTime <= 0)
            throw new IgniteSpiException("Invalid wait time, value greater than zero is expected: " + resWaitTime);

        if (addrReqAttempts <= 0)
            throw new IgniteSpiException("Invalid number of address request attempts, " +
                "value greater than zero is expected: " + addrReqAttempts);

        if (ttl != null && (ttl < 0 || ttl > 255))
            throw new IgniteSpiException("Time-to-live value is out of 0 <= TTL <= 255 range: " + ttl);

        if (F.isEmpty(getRegisteredAddresses()))
            U.warn(log, "TcpDiscoveryMulticastIpFinder has no pre-configured addresses " +
                "(it is recommended in production to specify at least one address in " +
                "TcpDiscoveryMulticastIpFinder.getAddresses() configuration property)");

        boolean clientMode = discoveryClientMode();

        InetAddress mcastAddr;

        try {
            mcastAddr = InetAddress.getByName(mcastGrp);
        }
        catch (UnknownHostException e) {
            throw new IgniteSpiException("Unknown multicast group: " + mcastGrp, e);
        }

        if (!mcastAddr.isMulticastAddress())
            throw new IgniteSpiException("Invalid multicast group address: " + mcastAddr);

        Collection<String> locAddrs;

        try {
            locAddrs = U.resolveLocalAddresses(U.resolveLocalHost(locAddr)).get1();
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to resolve local addresses [locAddr=" + locAddr + ']', e);
        }

        assert locAddrs != null;

        addrSnds = new ArrayList<>(locAddrs.size());

        Set<InetAddress> reqItfs = new HashSet<>(locAddrs.size()); // Interfaces used to send requests.

        for (String locAddr : locAddrs) {
            InetAddress addr;

            try {
                addr = InetAddress.getByName(locAddr);
            }
            catch (UnknownHostException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to resolve local address [locAddr=" + locAddr + ", err=" + e + ']');

                continue;
            }

            if (!addr.isLoopbackAddress()) {
                try {
                    if (!clientMode)
                        addrSnds.add(new AddressSender(mcastAddr, addr, addrs));

                    reqItfs.add(addr);
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to create multicast socket [mcastAddr=" + mcastAddr +
                            ", mcastGrp=" + mcastGrp + ", mcastPort=" + mcastPort + ", locAddr=" + addr +
                            ", err=" + e + ']');
                }
            }
        }

        boolean mcastErr = false;

        if (!clientMode) {
            if (addrSnds.isEmpty()) {
                try {
                    // Create non-bound socket if local host is loopback or failed to create sockets explicitly
                    // bound to interfaces.
                    addrSnds.add(new AddressSender(mcastAddr, null, addrs));
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to create multicast socket [mcastAddr=" + mcastAddr +
                            ", mcastGrp=" + mcastGrp + ", mcastPort=" + mcastPort + ", err=" + e + ']');
                }

                if (addrSnds.isEmpty()) {
                    try {
                        addrSnds.add(new AddressSender(mcastAddr, mcastAddr, addrs));

                        reqItfs.add(mcastAddr);
                    }
                    catch (IOException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to create multicast socket [mcastAddr=" + mcastAddr +
                                ", mcastGrp=" + mcastGrp + ", mcastPort=" + mcastPort + ", locAddr=" + mcastAddr +
                                ", err=" + e + ']');
                    }
                }
            }

            if (!addrSnds.isEmpty()) {
                for (AddressSender addrSnd : addrSnds)
                    addrSnd.start();
            }
            else
                mcastErr = true;
        }
        else
            assert addrSnds.isEmpty() : addrSnds;

        Collection<InetSocketAddress> ret;

        if (reqItfs.size() > 1) {
            ret = new HashSet<>();

            Collection<AddressReceiver> rcvrs = new ArrayList<>();

            for (InetAddress itf : reqItfs) {
                AddressReceiver rcvr = new AddressReceiver(mcastAddr, itf);

                rcvr.start();

                rcvrs.add(rcvr);
            }

            for (AddressReceiver rcvr : rcvrs) {
                try {
                    rcvr.join();

                    ret.addAll(rcvr.addresses());
                }
                catch (InterruptedException ignore) {
                    U.warn(log, "Got interrupted while receiving address request.");

                    Thread.currentThread().interrupt();

                    break;
                }
            }
        }
        else {
            T2<Collection<InetSocketAddress>, Boolean> res = requestAddresses(mcastAddr, F.first(reqItfs));

            ret = res.get1();

            mcastErr |= res.get2();
        }

        if (ret.isEmpty()) {
            if (mcastErr) {
                if (getRegisteredAddresses().isEmpty()) {
                    InetSocketAddress addr = new InetSocketAddress("localhost", TcpDiscoverySpi.DFLT_PORT);

                    U.quietAndWarn(log, "TcpDiscoveryMulticastIpFinder failed to initialize multicast, " +
                        "will use default address: " + addr);

                    registerAddresses(Collections.singleton(addr));
                }
                else
                    U.quietAndWarn(log, "TcpDiscoveryMulticastIpFinder failed to initialize multicast, " +
                        "will use pre-configured addresses.");
            }
        }
        else
            registerAddresses(ret);
    }

    /** {@inheritDoc} */
    @Override public void onSpiContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException {
        super.onSpiContextInitialized(spiCtx);

        spiCtx.registerPort(mcastPort, UDP);
    }

    /**
     * Sends multicast address request message and waits for reply. Response wait time and number
     * of request attempts are configured as properties {@link #setResponseWaitTime} and
     * {@link #setAddressRequestAttempts}.
     *
     * @param mcastAddr Multicast address where to send request.
     * @param sockItf Optional interface multicast socket should be bound to.
     * @return Tuple where first value is collection of received addresses, second is boolean which is
     *      {@code true} if got error on send.
     */
    private T2<Collection<InetSocketAddress>, Boolean> requestAddresses(InetAddress mcastAddr,
        @Nullable InetAddress sockItf)
    {
        Collection<InetSocketAddress> rmtAddrs = new HashSet<>();

        boolean sndErr = false;

        try {
            DatagramPacket reqPckt = new DatagramPacket(MSG_ADDR_REQ_DATA, MSG_ADDR_REQ_DATA.length,
                mcastAddr, mcastPort);

            byte[] resData = new byte[AddressResponse.MAX_DATA_LENGTH];

            DatagramPacket resPckt = new DatagramPacket(resData, resData.length);

            boolean sndError = false;

            for (int i = 0; i < addrReqAttempts; i++) {
                MulticastSocket sock = null;

                try {
                    sock = new MulticastSocket(0);

                    // Use 'false' to enable support for more than one node on the same machine.
                    sock.setLoopbackMode(false);

                    if (sockItf != null)
                        sock.setInterface(sockItf);

                    sock.setSoTimeout(resWaitTime);

                    if (ttl != null)
                        sock.setTimeToLive(ttl);

                    reqPckt.setData(MSG_ADDR_REQ_DATA);

                    try {
                        sock.send(reqPckt);
                    }
                    catch (IOException e) {
                        sndErr = true;

                        if (!handleNetworkError(e))
                            break;

                        if (i < addrReqAttempts - 1) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send multicast address request (will retry in 500 ms): " + e);

                            U.sleep(500);
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send multicast address request: " + e);
                        }

                        sndError = true;

                        continue;
                    }

                    long rcvEnd = U.currentTimeMillis() + resWaitTime;

                    try {
                        while (U.currentTimeMillis() < rcvEnd) { // Try to receive multiple responses.
                            sock.receive(resPckt);

                            byte[] data = resPckt.getData();

                            if (!U.bytesEqual(U.IGNITE_HEADER, 0, data, 0, U.IGNITE_HEADER.length)) {
                                U.error(log, "Failed to verify message header.");

                                continue;
                            }

                            AddressResponse addrRes;

                            try {
                                addrRes = new AddressResponse(data);
                            }
                            catch (IgniteCheckedException e) {
                                LT.warn(log, e, "Failed to deserialize multicast response.");

                                continue;
                            }

                            rmtAddrs.addAll(addrRes.addresses());
                        }
                    }
                    catch (SocketTimeoutException ignored) {
                        if (log.isDebugEnabled()) // DatagramSocket.receive timeout has expired.
                            log.debug("Address receive timeout.");
                    }
                }
                catch (IOException e) {
                    U.error(log, "Failed to request nodes addresses.", e);
                }
                finally {
                    U.close(sock);
                }

                if (!rmtAddrs.isEmpty())
                    break;

                if (i < addrReqAttempts - 1) // Wait some time before re-sending address request.
                    U.sleep(200);
            }

            if (log.isDebugEnabled())
                log.debug("Received nodes addresses: " + rmtAddrs);

            if (rmtAddrs.isEmpty() && sndError)
                U.quietAndWarn(log, "Failed to send multicast message (is multicast enabled on this node?).");

            return new T2<>(rmtAddrs, sndErr);
        }
        catch (IgniteInterruptedCheckedException ignored) {
            U.warn(log, "Got interrupted while sending address request.");

            Thread.currentThread().interrupt();

            return new T2<>(rmtAddrs, sndErr);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (addrSnds != null) {
            for (AddressSender addrSnd : addrSnds)
                U.interrupt(addrSnd);

            for (AddressSender addrSnd : addrSnds)
                U.join(addrSnd, log);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryMulticastIpFinder.class, this, "super", super.toString());
    }

    /**
     * @param e Network error to handle.
     * @return {@code True} if this error is recoverable and the operation can be retried.
     */
    private boolean handleNetworkError(IOException e) {
        if ("Network is unreachable".equals(e.getMessage()) && U.isMacOs()) {
            U.warn(log, "Multicast does not work on Mac OS JVM loopback address (configure external IP address " +
                "for 'localHost' configuration property)");

            return false;
        }

        return true;
    }

    /**
     * Response to multicast address request.
     */
    private static class AddressResponse {
        /** Maximum supported multicast message. */
        public static final int MAX_DATA_LENGTH = 64 * 1024;

        /** */
        private byte[] data;

        /** */
        private Collection<InetSocketAddress> addrs;

        /**
         * @param addrs Addresses discovery SPI binds to.
         * @throws IgniteCheckedException If marshalling failed.
         */
        private AddressResponse(Collection<InetSocketAddress> addrs) throws IgniteCheckedException {
            this.addrs = addrs;

            byte[] addrsData = marsh.marshal(addrs);
            data = new byte[U.IGNITE_HEADER.length + addrsData.length];

            if (data.length > MAX_DATA_LENGTH)
                throw new IgniteCheckedException("Too long data packet [size=" + data.length + ", max=" + MAX_DATA_LENGTH + "]");

            System.arraycopy(U.IGNITE_HEADER, 0, data, 0, U.IGNITE_HEADER.length);
            System.arraycopy(addrsData, 0, data, 4, addrsData.length);
        }

        /**
         * @param data Message data.
         * @throws IgniteCheckedException If unmarshalling failed.
         */
        private AddressResponse(byte[] data) throws IgniteCheckedException {
            assert U.bytesEqual(U.IGNITE_HEADER, 0, data, 0, U.IGNITE_HEADER.length);

            this.data = data;

            addrs = marsh.unmarshal(Arrays.copyOfRange(data, U.IGNITE_HEADER.length, data.length), null);
        }

        /**
         * @return Message data.
         */
        byte[] data() {
            return data;
        }

        /**
         * @return IP address discovery SPI binds to.
         */
        public Collection<InetSocketAddress> addresses() {
            return addrs;
        }
    }

    /**
     * Thread sends multicast address request message and waits for reply.
     */
    private class AddressReceiver extends IgniteSpiThread {
        /** */
        private final InetAddress mcastAddr;

        /** */
        private final InetAddress sockAddr;

        /** */
        private Collection<InetSocketAddress> addrs;

        /**
         * @param mcastAddr Multicast address where to send request.
         * @param sockAddr Optional address multicast socket should be bound to.
         */
        private AddressReceiver(InetAddress mcastAddr, InetAddress sockAddr) {
            super(ignite == null ? null : ignite.name(), "tcp-disco-multicast-addr-rcvr", log);
            this.mcastAddr = mcastAddr;
            this.sockAddr = sockAddr;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            addrs = requestAddresses(mcastAddr, sockAddr).get1();
        }

        /**
         * @return Received addresses.
         */
        Collection<InetSocketAddress> addresses() {
            return addrs;
        }
    }

    /**
     * Thread listening for multicast address requests and sending response
     * containing socket address this node's discovery SPI listens to.
     */
    private class AddressSender extends IgniteSpiThread {
        /** */
        @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
        private MulticastSocket sock;

        /** */
        private final InetAddress mcastGrp;

        /** */
        private final Collection<InetSocketAddress> addrs;

        /** */
        private final InetAddress sockItf;

        /**
         * @param mcastGrp Multicast address.
         * @param sockItf Optional interface multicast socket should be bound to.
         * @param addrs Local node addresses.
         * @throws IOException If fails to create multicast socket.
         */
        private AddressSender(InetAddress mcastGrp, @Nullable InetAddress sockItf, Collection<InetSocketAddress> addrs)
            throws IOException {
            super(ignite == null ? null : ignite.name(), "tcp-disco-multicast-addr-sender", log);
            this.mcastGrp = mcastGrp;
            this.addrs = addrs;
            this.sockItf = sockItf;

            sock = createSocket();
        }

        /**
         * Creates multicast socket and joins multicast group.
         *
         * @throws IOException If fails to create socket or join multicast group.
         * @return Multicast socket.
         */
        private MulticastSocket createSocket() throws IOException {
            MulticastSocket sock = new MulticastSocket(mcastPort);

            sock.setLoopbackMode(false); // Use 'false' to enable support for more than one node on the same machine.

            if (sockItf != null)
                sock.setInterface(sockItf);

            if (sock.getLoopbackMode())
                U.warn(log, "Loopback mode is disabled which prevents nodes on the same machine from discovering " +
                    "each other.");

            sock.joinGroup(mcastGrp);

            if (ttl != null)
                sock.setTimeToLive(ttl);

            return sock;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            AddressResponse res;

            try {
                res = new AddressResponse(addrs);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to prepare multicast message.", e);

                return;
            }

            byte[] reqData = new byte[MSG_ADDR_REQ_DATA.length];

            DatagramPacket pckt = new DatagramPacket(reqData, reqData.length);

            while (!isInterrupted()) {
                try {
                    MulticastSocket sock;

                    synchronized (this) {
                        if (isInterrupted())
                            return;

                        sock = this.sock;

                        if (sock == null)
                            sock = createSocket();
                    }

                    sock.receive(pckt);

                    if (!U.bytesEqual(U.IGNITE_HEADER, 0, reqData, 0, U.IGNITE_HEADER.length)) {
                        U.error(log, "Failed to verify message header.");

                        continue;
                    }

                    try {
                        sock.send(new DatagramPacket(res.data(), res.data().length, pckt.getAddress(), pckt.getPort()));
                    }
                    catch (IOException e) {
                        if (e.getMessage().contains("Operation not permitted")) {
                            if (log.isDebugEnabled())
                                log.debug("Got 'operation not permitted' error, ignoring: " + e);
                        }
                        else
                            throw e;
                    }
                }
                catch (IOException e) {
                    if (!isInterrupted()) {
                        LT.warn(log, e, "Failed to send/receive address message (will try to reconnect).");

                        synchronized (this) {
                            U.close(sock);

                            sock = null;
                        }
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void interrupt() {
            super.interrupt();

            synchronized (this) {
                U.close(sock);

                sock = null;
            }
        }

        /** {@inheritDoc} */
        @Override protected void cleanup() {
            synchronized (this) {
                U.close(sock);

                sock = null;
            }
        }
    }
}