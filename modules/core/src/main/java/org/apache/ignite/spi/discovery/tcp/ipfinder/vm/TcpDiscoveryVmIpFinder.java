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

package org.apache.ignite.spi.discovery.tcp.ipfinder.vm;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TCP_DISCOVERY_ADDRESSES;

/**
 * IP Finder which works only with pre-configured list of IP addresses specified
 * via {@link #setAddresses(Collection)} method. By default, this IP finder is
 * not {@code shared}, which means that all grid nodes have to be configured with the
 * same list of IP addresses when this IP finder is used.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Addresses for initialization (see {@link #setAddresses(Collection)})</li>
 *      <li>Shared flag (see {@link #setShared(boolean)})</li>
 * </ul>
 */
public class TcpDiscoveryVmIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Addresses from the configuration or IGNITE_TCP_DISCOVERY_ADDRESSES. */
    @GridToStringInclude
    private Collection<String> addrs = new ArrayList<>();

    /** Registered InetSocketAddresses. */
    @GridToStringInclude
    private Collection<InetSocketAddress> registeredAddrs = new LinkedHashSet<>();

    /**
     * Initialize from system property.
     */
    {
        String ips = IgniteSystemProperties.getString(IGNITE_TCP_DISCOVERY_ADDRESSES);

        if (!F.isEmpty(ips)) {
            for (String s : ips.split(",")) {
                if (!F.isEmpty(s)) {
                    s = s.trim();

                    if (!F.isEmpty(s)) {
                        try {
                            addrs.add(s);
                        }
                        catch (IgniteSpiException e) {
                            throw new IgniteException(e);
                        }
                    }
                }
            }
        }
    }

    /**
     * Constructs new IP finder.
     */
    public TcpDiscoveryVmIpFinder() {
        // No-op.
    }

    /**
     * Constructs new IP finder.
     *
     * @param shared {@code true} if IP finder is shared.
     * @see #setShared(boolean)
     */
    public TcpDiscoveryVmIpFinder(boolean shared) {
        setShared(shared);
    }

    /**
     * Parses provided values and initializes the internal collection of addresses.
     * <p>
     * Addresses may be represented as follows:
     * <ul>
     *     <li>IP address (e.g. 127.0.0.1, 9.9.9.9, etc);</li>
     *     <li>IP address and port (e.g. 127.0.0.1:47500, 9.9.9.9:47501, etc);</li>
     *     <li>IP address and port range (e.g. 127.0.0.1:47500..47510, 9.9.9.9:47501..47504, etc);</li>
     *     <li>Hostname (e.g. host1.com, host2, etc);</li>
     *     <li>Hostname and port (e.g. host1.com:47500, host2:47502, etc).</li>
     *     <li>Hostname and port range (e.g. host1.com:47500..47510, host2:47502..47508, etc).</li>
     * </ul>
     * <p>
     * If port is 0 or not provided then default port will be used (depends on
     * discovery SPI configuration).
     * <p>
     * If port range is provided (e.g. host:port1..port2) the following should be considered:
     * <ul>
     *     <li>{@code port1 < port2} should be {@code true};</li>
     *     <li>Both {@code port1} and {@code port2} should be greater than {@code 0}.</li>
     * </ul>
     *
     * @param addrs Known nodes addresses.
     * @throws IgniteSpiException If any error occurs.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public synchronized TcpDiscoveryVmIpFinder setAddresses(Collection<String> addrs) throws IgniteSpiException {
        if (F.isEmpty(addrs))
            return this;

        // Validate the spot that addresses can be resolved, to preserve the existing behavior.
        for (String ipStr : addrs)
            address(ipStr);

        this.addrs = new ArrayList<>(addrs);

        return this;
    }

    /**
     * Creates address from string.
     *
     * @param ipStr Address string.
     * @return Socket addresses (may contain 1 or more addresses if provided string
     *      includes port range).
     * @throws IgniteSpiException If failed.
     */
    private static Collection<InetSocketAddress> address(String ipStr) throws IgniteSpiException {
        ipStr = ipStr.trim();

        String errMsg = "Failed to parse provided address: " + ipStr;

        int colonCnt = ipStr.length() - ipStr.replace(":", "").length();

        if (colonCnt > 1) {
            // IPv6 address (literal IPv6 addresses are enclosed in square brackets, for example
            // https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443).
            if (ipStr.startsWith("[")) {
                ipStr = ipStr.substring(1);

                if (ipStr.contains("]:"))
                    return addresses(ipStr, "\\]\\:", errMsg);
                else if (ipStr.endsWith("]"))
                    ipStr = ipStr.substring(0, ipStr.length() - 1);
                else
                    throw new IgniteSpiException(errMsg);
            }
        }
        else {
            // IPv4 address.
            if (ipStr.endsWith(":"))
                ipStr = ipStr.substring(0, ipStr.length() - 1);
            else if (ipStr.indexOf(':') >= 0)
                return addresses(ipStr, "\\:", errMsg);
        }

        Collection<InetSocketAddress> col = new LinkedHashSet<>();

        try {
            InetAddress[] inetAddresses = InetAddress.getAllByName(ipStr);

            for (InetAddress addrs : inetAddresses)
                col.add(new InetSocketAddress(addrs, 0));
        }
        catch (UnknownHostException ignored) {
            // Preserve existing behavior on UnknownHostException
            col = Collections.singleton(new InetSocketAddress(ipStr, 0));
        }

        return col;
    }

    /**
     * Creates address from string with port information.
     *
     * @param ipStr Address string
     * @param regexDelim Port regex delimiter.
     * @param errMsg Error message.
     * @return Socket addresses (may contain 1 or more addresses if provided string
     *      includes port range).
     * @throws IgniteSpiException If failed.
     */
    private static Collection<InetSocketAddress> addresses(String ipStr, String regexDelim, String errMsg)
        throws IgniteSpiException {
        String[] tokens = ipStr.split(regexDelim);

        if (tokens.length == 2) {
            String addrStr = tokens[0];
            String portStr = tokens[1];
            int port1, port2;

            if (portStr.contains("..")) {
                port1 = Integer.parseInt(portStr.substring(0, portStr.indexOf("..")));
                port2 = Integer.parseInt(portStr.substring(portStr.indexOf("..") + 2, portStr.length()));
            }
            else
                port1 = port2 = Integer.parseInt(portStr);

            if ((port1 != port2 && port2 < port1) || port1 <= 0 || port2 <= 0)
                throw new IgniteSpiException(errMsg);

            try {
                Collection<InetSocketAddress> res = new ArrayList<>();

                InetAddress[] inetAddresses;

                try {
                    inetAddresses = InetAddress.getAllByName(addrStr);
                }
                catch (UnknownHostException e) {
                    // Ignore
                    for (int i = port1; i <= port2; i++)
                        res.add(new InetSocketAddress(addrStr, i));

                    return res;
                }

                for (InetAddress curAddr : inetAddresses) {
                    // Upper bound included.
                    for (int i = port1; i <= port2; i++)
                        res.add(new InetSocketAddress(curAddr, i));
                }

                return res;
            }
            catch (IllegalArgumentException e) {
                throw new IgniteSpiException(errMsg, e);
            }
        }
        else
            throw new IgniteSpiException(errMsg);
    }

    /** {@inheritDoc} */
    @Override public synchronized Collection<InetSocketAddress> getRegisteredAddresses() {
        Collection<InetSocketAddress> resolvedAddrs = new LinkedHashSet<>();

        for (String ipStr : addrs)
            resolvedAddrs.addAll(address(ipStr));

        resolvedAddrs.addAll(registeredAddrs);

        return resolvedAddrs;
    }

    /** {@inheritDoc} */
    @Override public synchronized void registerAddresses(Collection<InetSocketAddress> addrs) {
        assert !F.isEmpty(addrs);

        if (!F.isEmpty(addrs))
            this.registeredAddrs.addAll(addrs);
    }

    /** {@inheritDoc} */
    @Override public synchronized void unregisterAddresses(Collection<InetSocketAddress> addrs) {
        assert !F.isEmpty(addrs);

        if (!F.isEmpty(addrs))
            this.registeredAddrs.removeAll(addrs);
    }

    /** {@inheritDoc} */
    @Override public TcpDiscoveryVmIpFinder setShared(boolean shared) {
        super.setShared(shared);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryVmIpFinder.class, this, "super", super.toString());
    }
}
