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

package org.apache.ignite.network;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A representation of a network address including a host name and a port.
 */
public class NetworkAddress implements Serializable {
    /** Regexp for parsing strings in the "host:port" format. */
    private static final Pattern ADDRESS_PATTERN = Pattern.compile("(.+):(\\d+)");

    /** Host. */
    private final String host;

    /** Port. */
    private final int port;

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     */
    public NetworkAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Parses a {@code NetworkAddress} from a string in the "host:port" format.
     *
     * @param addrStr String in "host:port" format.
     * @return Parsed address.
     * @throws IllegalArgumentException If the provided string does not match the required format.
     */
    public static NetworkAddress from(String addrStr) {
        Matcher matcher = ADDRESS_PATTERN.matcher(addrStr);

        if (!matcher.matches()) {
            throw new IllegalArgumentException("Unable to parse the network address from: " + addrStr);
        }

        String host = matcher.group(1);

        String portStr = matcher.group(2);

        int port;

        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Illegal port format: " + portStr, ex);
        }

        return new NetworkAddress(host, port);
    }

    /**
     * Creates a {@code NetworkAddress} from a {@link InetSocketAddress}.
     *
     * @param addr Address.
     * @return Created network address.
     */
    public static NetworkAddress from(InetSocketAddress addr) {
        return new NetworkAddress(addr.getHostName(), addr.getPort());
    }

    /**
     * Returns the host name.
     *
     * @return Host name.
     */
    public String host() {
        return host;
    }

    /**
     * Returns the network port.
     *
     * @return Port.
     */
    public int port() {
        return port;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NetworkAddress address = (NetworkAddress) o;
        return port == address.port && host.equals(address.host);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return host + ":" + port;
    }
}
