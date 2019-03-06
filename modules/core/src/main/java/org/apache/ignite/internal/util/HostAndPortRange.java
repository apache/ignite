/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util;

import java.io.Serializable;
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

        int portFrom;
        int portTo;

        final int colIdx = addrStr.indexOf(':');

        if (colIdx > 0) {
            String portFromStr;
            String portToStr;

            host = addrStr.substring(0, colIdx);

            String portStr = addrStr.substring(colIdx + 1, addrStr.length());

            if (F.isEmpty(portStr))
                throw createParseError(addrStr, errMsgPrefix, "port range is not specified");

            int portRangeIdx = portStr.indexOf("..");

            if (portRangeIdx >= 0) {
                // Port range is specified.
                portFromStr = portStr.substring(0, portRangeIdx);
                portToStr = portStr.substring(portRangeIdx + 2, portStr.length());
            }
            else {
                // Single port is specified.
                portFromStr = portStr;
                portToStr = portStr;
            }

            portFrom = parsePort(portFromStr, addrStr, errMsgPrefix);
            portTo = parsePort(portToStr, addrStr, errMsgPrefix);

            if (portFrom > portTo)
                throw createParseError(addrStr, errMsgPrefix, "start port cannot be less than end port");
        }
        else {
            // Host name not specified.
            if (colIdx == 0)
                throw createParseError(addrStr, errMsgPrefix, "Host name is empty");

            // Port is not specified, use defaults.
            host = addrStr;

            portFrom = dfltPortFrom;
            portTo = dfltPortTo;
        }

        if (F.isEmpty(host))
            throw createParseError(addrStr, errMsgPrefix, "Host name is empty");

        return new HostAndPortRange(host, portFrom, portTo);
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
