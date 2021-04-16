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

package org.apache.ignite.internal.util;

import java.io.Serializable;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Represents address along with port range.
 */
public class HostAndPortRange implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Host. */
    private final String host;

    /** Port from. */
    private final int portFrom;

    /** Port to. */
    private final int portTo;

    /**
     * Parse string into host and port pair.
     *
     * @param addrStr String.
     * @param dfltPortFrom Default port from.
     * @param dfltPortTo Default port to.
     * @param errMsgPrefix Error message prefix.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public static HostAndPortRange parse(String addrStr, int dfltPortFrom, int dfltPortTo, String errMsgPrefix)
        throws IgniteCheckedException {
        assert dfltPortFrom <= dfltPortTo;

        String host;

        String portStr;
        int portFrom;
        int portTo;

        if (F.isEmpty(addrStr))
            throw createParseError(addrStr, errMsgPrefix, "Address is empty");

        if (addrStr.charAt(0) == '[') { // IPv6 with port(s)
            int hostEndIdx = addrStr.indexOf(']');
            
            if (hostEndIdx == -1)
                throw createParseError(addrStr, errMsgPrefix, "Failed to parse IPv6 address, missing ']'");

            host = addrStr.substring(1, hostEndIdx);
            
            if (hostEndIdx == addrStr.length() - 1) { // no port specified, using default
                portFrom = dfltPortFrom;
                portTo = dfltPortTo;
            }
            else { // port specified
                portStr = addrStr.substring(hostEndIdx + 2);

                int[] ports = verifyPortStr(addrStr, errMsgPrefix, portStr);
                portFrom = ports[0];
                portTo = ports[1];
            }
        }
        else { // IPv4 || IPv6 without port || empty host
            final int colIdx = addrStr.lastIndexOf(':');
            
            if (colIdx > 0) {
                if (addrStr.lastIndexOf(':', colIdx - 1) != -1) { // IPv6 without [] and port
                    try {
                        Inet6Address.getByName(addrStr);
                        host = addrStr;
                        portFrom = dfltPortFrom;
                        portTo = dfltPortTo;
                    }
                    catch (UnknownHostException e) {
                        throw createParseError(addrStr, errMsgPrefix, "IPv6 is incorrect", e);
                    }
                }
                else {
                    host = addrStr.substring(0, colIdx);
                    portStr = addrStr.substring(colIdx + 1);
                    int[] ports = verifyPortStr(addrStr, errMsgPrefix, portStr);
                    portFrom = ports[0];
                    portTo = ports[1];
                }
            }
            else if (colIdx == 0)
                throw createParseError(addrStr, errMsgPrefix, "Host name is empty");
            else { // Port is not specified, use defaults.
                host = addrStr;

                portFrom = dfltPortFrom;
                portTo = dfltPortTo;
            }
        }

        return new HostAndPortRange(host, portFrom, portTo);
    }

    /**
     * Verifies string containing single port or ports range.
     *
     * @param addrStr Address String.
     * @param errMsgPrefix Error message prefix.
     * @param portStr Port or port range string.
     * @return Array of int[portFrom, portTo].
     * @throws IgniteCheckedException If failed.
     */
    private static int[] verifyPortStr(String addrStr, String errMsgPrefix, String portStr)
        throws IgniteCheckedException {
        String portFromStr;
        String portToStr;

        if (F.isEmpty(portStr))
            throw createParseError(addrStr, errMsgPrefix, "port range is not specified");

        int portRangeIdx = portStr.indexOf("..");

        if (portRangeIdx >= 0) {
            // Port range is specified.
            portFromStr = portStr.substring(0, portRangeIdx);
            portToStr = portStr.substring(portRangeIdx + 2);
        }
        else {
            // Single port is specified.
            portFromStr = portStr;
            portToStr = portStr;
        }

        int portFrom = parsePort(portFromStr, addrStr, errMsgPrefix);
        int portTo = parsePort(portToStr, addrStr, errMsgPrefix);

        if (portFrom > portTo)
            throw createParseError(addrStr, errMsgPrefix, "start port cannot be less than end port");

        return new int[] {portFrom, portTo};
    }

    /**
     * Parse port.
     *
     * @param portStr Port string.
     * @param addrStr Address string.
     * @param errMsgPrefix Error message prefix.
     * @return Parsed port.
     * @throws IgniteCheckedException If failed.
     */
    private static int parsePort(String portStr, String addrStr, String errMsgPrefix) throws IgniteCheckedException {
        try {
            int port = Integer.parseInt(portStr);

            if (port <= 0 || port > 65535)
                throw createParseError(addrStr, errMsgPrefix, "port range contains invalid port " + portStr);

            return port;
        }
        catch (NumberFormatException ignored) {
            throw createParseError(addrStr, errMsgPrefix, "port range contains invalid port " + portStr);
        }
    }

    /**
     * Create parse error.
     *
     * @param addrStr Address string.
     * @param errMsgPrefix Error message prefix.
     * @param errMsg Error message.
     * @return Exception.
     */
    private static IgniteCheckedException createParseError(String addrStr, String errMsgPrefix, String errMsg) {
        return new IgniteCheckedException(errMsgPrefix + " (" + errMsg + "): " + addrStr);
    }

    /**
     * Create parse error with cause - nested exception.
     *
     * @param addrStr Address string.
     * @param errMsgPrefix Error message prefix.
     * @param errMsg Error message.
     * @param cause Cause exception.
     * @return Exception.
     */
    private static IgniteCheckedException createParseError(String addrStr, String errMsgPrefix, String errMsg, Throwable cause) {
        return new IgniteCheckedException(errMsgPrefix + " (" + errMsg + "): " + addrStr, cause);
    }

    /**
     * Constructor.
     *
     * @param host Host.
     * @param port Port.
     */
    public HostAndPortRange(String host, int port) {
        this(host, port, port);
    }

    /**
     * Constructor.
     *
     * @param host Host.
     * @param portFrom Port from.
     * @param portTo Port to.
     */
    public HostAndPortRange(String host, int portFrom, int portTo) {
        assert !F.isEmpty(host);
        assert portFrom <= portTo;

        this.host = host;
        this.portFrom = portFrom;
        this.portTo = portTo;
    }

    /**
     * @return Host.
     */
    public String host() {
        return host;
    }

    /**
     * @return Port from.
     */
    public int portFrom() {
        return portFrom;
    }

    /**
     * @return Port to.
     */
    public int portTo() {
        return portTo;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o != null && o instanceof HostAndPortRange) {
            HostAndPortRange other = (HostAndPortRange)o;

            return F.eq(host, other.host) && portFrom == other.portFrom && portTo == other.portTo;
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = host.hashCode();

        res = 31 * res + portFrom;
        res = 31 * res + portTo;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return host + ":" + (portFrom == portTo ? portFrom : portFrom + ".." + portTo);
    }
}
