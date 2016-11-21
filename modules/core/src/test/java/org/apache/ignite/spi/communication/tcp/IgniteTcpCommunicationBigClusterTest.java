package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Testing {@link TcpCommunicationSpi} under big cluster conditions (long DiscoverySpi delivery)
 *
 * @author Alexandr Kuramshin <ein.nsk.ru@gmail.com>
 */
public class IgniteTcpCommunicationBigClusterTest extends GridCommonAbstractTest {

    /** */
    private static final int IGNITE_NODES_NUMBER = 3;

    /** */
    private static final long COMMUNICATION_TIMEOUT = 1_000L;

    /** Should be about of the COMMUNICATION_TIMEOUT value to get the error */
    private static final long ADDED_MESSAGE_DELAY = 10 * COMMUNICATION_TIMEOUT;

    /** */
    private static final long RUNNING_TIMESPAN = 3600_1000L;

    /** */
    private static final long BROADCAST_PERIOD = 100L;

    /** */
    private static final String CONTROL_ANSWER = "ignite";

    /** */
    private static final Logger LOGGER = Logger.getLogger(IgniteTcpCommunicationBigClusterTest.class.getName());

    /** */
    private static final Level LOG_LEVEL = Level.SEVERE;

    /** */
    private CountDownLatch startLatch;

    @Override protected long getTestTimeout() {
        return Math.max(super.getTestTimeout(), RUNNING_TIMESPAN * 2);
    }

    /** */
    private static IgniteConfiguration config(String gridName) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setGridName(gridName);
        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discovery = new SlowTcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47510"));
        discovery.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discovery);

        TcpCommunicationSpi communication = new TcpCommunicationSpi();
        communication.setConnectTimeout(COMMUNICATION_TIMEOUT);
        communication.setMaxConnectTimeout(COMMUNICATION_TIMEOUT);
        communication.setReconnectCount(1);
        cfg.setCommunicationSpi(communication);

        return cfg;
    }

    /** */
    private static void println(String str) {
        LOGGER.log(LOG_LEVEL, str);
        logCounters();
    }

    /** */
    private static void println(String str, Throwable ex) {
        LOGGER.log(LOG_LEVEL, str, ex);
        logCounters();
    }

    /** */
    private static void printf(String format, Object... args) {
        LOGGER.log(LOG_LEVEL, MessageFormat.format(format, args));
        logCounters();
    }

    /** */
    private static void printf(String format, Throwable ex, Object... args) {
        LOGGER.log(LOG_LEVEL, MessageFormat.format(format, args), ex);
        logCounters();
    }

    /** */
    private static void logCounters() {
        LOGGER.log(LOG_LEVEL, MessageFormat.format(
            "joinTopology: started = {0}, active = {1}; getSpiContext: started = {2}, active = {3}",
            TcpDiscoverySpi.JOIN_TOPOLOGY_STARTED_COUNT.get(),
            TcpDiscoverySpi.JOIN_TOPOLOGY_ACTIVE_COUNT.get(),
            TcpCommunicationSpi.STARTED_COUNT.get(),
            TcpCommunicationSpi.ACTIVE_COUNT.get()));
    }

    /** */
    public synchronized void testBigCluster() throws Exception {
        startLatch = new CountDownLatch(IGNITE_NODES_NUMBER);
        final ExecutorService execSvc = Executors.newCachedThreadPool();
        for (int i = 0; i < IGNITE_NODES_NUMBER; ++i) {
            final String name = "testBigClusterNode-" + i;
            execSvc.submit(() -> {
                startNode(name);
            });
        }
        startLatch.await();
        println("All nodes running");
        Thread.sleep(RUNNING_TIMESPAN);
        println("Stopping all nodes");
        execSvc.shutdownNow();
        execSvc.awaitTermination(1, TimeUnit.MINUTES);
        println("Stopped all nodes");
    }

    /** */
    private void startNode(String name) {
        printf("Starting node = {0}", name);
        try (final Ignite ignite = Ignition.start(config(name))) {
            printf("Started node = {0}", name);
            startLatch.countDown();
            nodeWork(ignite);
            printf("Stopping node = {0}", name);
        }
        printf("Stopped node = {0}", name);
    }

    /** */
    private void nodeWork(final Ignite ignite) {
        try {
            int count = 0;
            for (; ; ) {
                Thread.sleep(BROADCAST_PERIOD);
                Collection<String> results = ignite.compute().broadcast(() -> {
                    return CONTROL_ANSWER;
                });
                for (String result : results)
                    if (!CONTROL_ANSWER.equals(result))
                        throw new IllegalArgumentException("Wrong answer from node: " + result);
                if (count != results.size())
                    printf("Computed results: node = {0}, count = {1}", ignite.name(), count = results.size());
            }
        }
        catch (InterruptedException | IgniteInterruptedException ex) {
            printf("Node thread interrupted: node = {0}", ignite.name());
        }
        catch (Throwable ex) {
            printf("Node thread exit on error: node = {0}", ex, ignite.name());
        }
    }

    /** */
    private static class SlowTcpDiscoverySpi extends TcpDiscoverySpi {

        /** */
        @Override protected boolean ensured(TcpDiscoveryAbstractMessage msg) {
            if (ADDED_MESSAGE_DELAY > 0 && msg instanceof TcpDiscoveryNodeAddFinishedMessage)
                try {
                    Thread.sleep(ADDED_MESSAGE_DELAY);
                }
                catch (InterruptedException | IgniteInterruptedException ex) {
                    println("Long delivery of TcpDiscoveryNodeAddFinishedMessage interrupted");
                    throw ex instanceof IgniteInterruptedException ? (IgniteInterruptedException)ex
                        : new IgniteInterruptedException((InterruptedException)ex);
                }
                catch (Throwable ex) {
                    println("Long delivery of TcpDiscoveryNodeAddFinishedMessage error", ex);
                    throw ex instanceof RuntimeException ? (RuntimeException)ex : new RuntimeException(ex);
                }
            return super.ensured(msg);
        }
    }
}
