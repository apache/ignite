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

package org.apache.ignite.internal.client;

import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests HostAndPortRange.
 */
public class HostAndPortRangeTest {
    /**
     * Tests correct input address with IPv4 host and port range.
     */
    @Test
    public void testParseIPv4WithPortRange() {
        String addrStr = "127.0.0.1:8080..8090";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("127.0.0.1", 8080, 8090);

        assertEquals(expected, actual);
    }

    /**
     * Tests correct input address with IPv4 host and single port.
     */
    @Test
    public void testParseIPv4WithSinglePort() {
        String addrStr = "127.0.0.1:8080";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("127.0.0.1", 8080, 8080);

        assertEquals(expected, actual);
    }

    /**
     * Tests correct input address with IPv4 host and no port.
     */
    @Test
    public void testParseIPv4NoPort() {
        String addrStr = "127.0.0.1";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("127.0.0.1", 18360, 18362);

        assertEquals(expected, actual);
    }

    /**
     * Tests correct input address with IPv6 host and port range.
     */
    @Test
    public void testParseIPv6WithPortRange() {
        String addrStr = "[::1]:8080..8090";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("::1", 8080, 8090);

        assertEquals(expected, actual);
    }

    /**
     * Tests correct input address with IPv6 host and single port.
     */
    @Test
    public void testParseIPv6WithSinglePort() {
        String addrStr = "[3ffe:2a00:100:7031::]:8080";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("3ffe:2a00:100:7031::", 8080, 8080);

        assertEquals(expected, actual);
    }

    /**
     * Tests correct input address with IPv6 host and no port.
     */
    @Test
    public void testParseIPv6NoPort() {
        String addrStr = "::FFFF:129.144.52.38";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("::FFFF:129.144.52.38", 18360, 18362);

        assertEquals(expected, actual);
    }

    /**
     * Tests incorrect input address with IPv6 host (no brackets) and port.
     */
    @Test
    public void testParseIPv6IncorrectHost() {
        String addrStr = "3ffe:2a00:100:7031";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        var ex = assertThrows(IgniteException.class,
                () -> HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix));

        assertTrue(ex.getMessage().contains("IPv6 is incorrect"), ex.getMessage());
    }

    /**
     * Tests empty host and port.
     */
    @Test
    public void testParseNoHost() {
        String addrStr = ":8080";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        var ex = assertThrows(IgniteException.class,
                () -> HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix));

        assertTrue(ex.getMessage().contains("Host name is empty"), ex.getMessage());
    }

    /**
     * Tests empty address string.
     */
    @Test
    public void testParseNoAddress() {
        String addrStr = "";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;

        var ex = assertThrows(IgniteException.class,
                () -> HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix));

        assertTrue(ex.getMessage().contains("Address is empty"), ex.getMessage());
    }
}
