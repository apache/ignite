/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.util;

import java.io.IOException;
import java.util.Collections;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Testing logging via {@link IgniteUtils#warnDevOnly(IgniteLogger, Object)}.
 */
public class IgniteDevOnlyLogTest extends TestCase {
    /** Check that dev-only messages appear in the log. */
    public void testDevOnlyQuietMessage() throws IOException {
        String oldQuietVal = System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "true");

        try (Ignite ignite = startNode()) {
            String msg = getMessage(ignite);
            IgniteUtils.warnDevOnly(ignite.log(), msg);
            assertTrue(readLog(ignite).contains(msg));
        }
        finally {
            setOrClearProperty(IgniteSystemProperties.IGNITE_QUIET, oldQuietVal);
        }
    }

    /** Check that dev-only messages appear in the log. */
    public void testDevOnlyVerboseMessage() throws IOException {
        String oldQuietVal = System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        try (Ignite ignite = startNode()) {
            String msg = getMessage(ignite);
            IgniteUtils.warnDevOnly(ignite.log(), msg);
            assertTrue(readLog(ignite).contains(msg));
        }
        finally {
            setOrClearProperty(IgniteSystemProperties.IGNITE_QUIET, oldQuietVal);
        }
    }

    /**
     * Check that {@link IgniteUtils#warnDevOnly(IgniteLogger, Object)}
     * doesn't print anything if {@link org.apache.ignite.IgniteSystemProperties#IGNITE_DEV_ONLY_LOGGING_DISABLED}
     * is set to {@code true}.
     */
    public void testDevOnlyDisabledProperty() throws IOException {
        String oldDevOnlyVal = System.setProperty(IgniteSystemProperties.IGNITE_DEV_ONLY_LOGGING_DISABLED, "true");

        try (Ignite ignite = startNode()) {
            String msg = getMessage(ignite);
            IgniteUtils.warnDevOnly(ignite.log(), msg);
            assertFalse(readLog(ignite).contains(msg));
        }
        finally {
            setOrClearProperty(IgniteSystemProperties.IGNITE_DEV_ONLY_LOGGING_DISABLED, oldDevOnlyVal);
        }

    }

    /** Sets a system property if the value is not null, or clears it if the value is null. */
    private void setOrClearProperty(String key, String val) {
        if (val != null)
            System.setProperty(key, val);
        else
            System.clearProperty(IgniteSystemProperties.IGNITE_QUIET);
    }

    /** Starts an Ignite node. */
    private Ignite startNode() throws IOException {
        IgniteConfiguration configuration = new IgniteConfiguration()
            .setIgniteInstanceName(IgniteDevOnlyLogTest.class.getName() + "Instance")
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryVmIpFinder()
                    .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                )
            );

        return Ignition.start(configuration);
    }

    /** Reads log of the given node to a string. */
    private String readLog(Ignite ignite) throws IOException {
        return IgniteUtils.readFileToString(ignite.log().fileName(), "UTF-8");
    }

    /** Returns a test message. */
    private String getMessage(Ignite ignite) {
        // use node id in the message to avoid interference with other tests
        return "My id is " + ignite.cluster().localNode().id();
    }
}
