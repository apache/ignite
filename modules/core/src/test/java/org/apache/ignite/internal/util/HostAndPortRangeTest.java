package org.apache.ignite.internal.util;

import org.apache.ignite.IgniteCheckedException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class HostAndPortRangeTest {

    @Test
    public void testParseIPv4WithPortRange() throws IgniteCheckedException {
        String addrStr = "127.0.0.1:8080..8090";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("127.0.0.1", 8080, 8090);
        assertEquals(expected, actual);
    }

    @Test
    public void testParseIPv4WithSinglePort() throws IgniteCheckedException {
        String addrStr = "127.0.0.1:8080";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("127.0.0.1", 8080, 8080);
        assertEquals(expected, actual);
    }

    @Test
    public void testParseIPv4NoPort() throws IgniteCheckedException {
        String addrStr = "127.0.0.1";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("127.0.0.1", 18360, 18362);
        assertEquals(expected, actual);
    }

    @Test
    public void testParseIPv6WithPortRange() throws IgniteCheckedException {
        String addrStr = "[::1]:8080..8090";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("::1", 8080, 8090);
        assertEquals(expected, actual);
    }

    @Test
    public void testParseIPv6WithSinglePort() throws IgniteCheckedException {
        String addrStr = "[3ffe:2a00:100:7031::]:8080";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("3ffe:2a00:100:7031::", 8080, 8080);
        assertEquals(expected, actual);
    }

    @Test
    public void testParseIPv6NoPort() throws IgniteCheckedException {
        String addrStr = "[::FFFF:129.144.52.38]";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
        HostAndPortRange expected = new HostAndPortRange("::FFFF:129.144.52.38", 18360, 18362);
        assertEquals(expected, actual);
    }

    @Test(expected = IgniteCheckedException.class)
    public void testParseIPv6IncorrectHost() throws IgniteCheckedException {
        String addrStr = "3ffe:2a00:100:7031:::8080";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);

    }

    @Test(expected = IgniteCheckedException.class)
    public void testParseNoHost() throws IgniteCheckedException {
        String addrStr = ":8080";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
    }

    @Test(expected = IgniteCheckedException.class)
    public void testParseNoAddress() throws IgniteCheckedException {
        String addrStr = "";
        String errMsgPrefix = "";
        int dfltPortFrom = 18360;
        int dfltPortTo = 18362;
        HostAndPortRange actual = HostAndPortRange.parse(addrStr, dfltPortFrom, dfltPortTo, errMsgPrefix);
    }
}
