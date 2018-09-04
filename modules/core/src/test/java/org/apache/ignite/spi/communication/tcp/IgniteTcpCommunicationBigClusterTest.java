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
package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteExceptionRegistry;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
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
    private static final int IGNITE_NODES_NUMBER = 10;

    /** */
    private static final long COMMUNICATION_TIMEOUT = 1000;

    /** Should be about of the COMMUNICATION_TIMEOUT value to get the error */
    private static final long ADDED_MESSAGE_DELAY = 2 * COMMUNICATION_TIMEOUT;

    /** */
    private static final long RUNNING_TIMESPAN = ADDED_MESSAGE_DELAY * IGNITE_NODES_NUMBER;

    /** */
    private static final long BROADCAST_PERIOD = 100L;

    /** */
    private static final String CONTROL_ANSWER = "ignite";

    /** */
    private static final Logger LOGGER = Logger.getLogger(IgniteTcpCommunicationBigClusterTest.class.getName());

    /** */
    private static final Level LOG_LEVEL = Level.FINE;

    /** */
    private CountDownLatch startLatch;

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

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setConnectTimeout(COMMUNICATION_TIMEOUT);
        commSpi.setMaxConnectTimeout(2 * COMMUNICATION_TIMEOUT);
        commSpi.setReconnectCount(1);
        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** */
    private static void println(String str) {
        LOGGER.log(LOG_LEVEL, str);
    }

    /** */
    private static void println(String str, Throwable ex) {
        LOGGER.log(LOG_LEVEL, str, ex);
    }

    /** */
    private static void printf(String format, Object... args) {
        LOGGER.log(LOG_LEVEL, MessageFormat.format(format, args));
    }

    /** */
    private static void printf(String format, Throwable ex, Object... args) {
        LOGGER.log(LOG_LEVEL, MessageFormat.format(format, args), ex);
    }

    /** */
    @Override protected long getTestTimeout() {
        return Math.max(super.getTestTimeout(), RUNNING_TIMESPAN * 2);
    }

    /** */
    public synchronized void testBigCluster() throws Exception {
        startLatch = new CountDownLatch(IGNITE_NODES_NUMBER);
        final ExecutorService execSvc = Executors.newCachedThreadPool();
        for (int i = 0; i < IGNITE_NODES_NUMBER; ++i) {
            final String name = "testBigClusterNode-" + i;
            execSvc.submit(new IgniteRunnable() {
                @Override public void run() {
                    startNode(name);
                }
            });
        }
        startLatch.await();
        println("All nodes running");
        Thread.sleep(RUNNING_TIMESPAN);
        println("Stopping all nodes");
        execSvc.shutdownNow();
        execSvc.awaitTermination(1, TimeUnit.MINUTES);
        println("Stopped all nodes");
        final IgniteExceptionRegistry exReg = IgniteExceptionRegistry.get();
        if (exReg.errorCount() > 0) {
            for (IgniteExceptionRegistry.ExceptionInfo info : exReg.getErrors(0L))
                if (info.error() instanceof IgniteCheckedException
                    && "HandshakeTimeoutException".equals(info.error().getClass().getSimpleName()))
                    throw new IgniteCheckedException("Test failed", info.error());
        }
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
                Collection<String> results = ignite.compute().broadcast(new IgniteCallable<String>() {
                    @Override public String call() throws Exception {
                        return CONTROL_ANSWER;
                    }
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
