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
package org.apache.ignite.p2p;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class P2PScanQueryUndeployTest extends GridCommonAbstractTest {
    /** Predicate classname. */
    private static final String PREDICATE_CLASSNAME = "org.apache.ignite.tests.p2p.AlwaysTruePredicate";

    /** */
    private static final String TEST_PREDICATE_RESOURCE_NAME = U.classNameToResourceName(PREDICATE_CLASSNAME);

    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";

    /** Client instance name. */
    private static final String CLIENT_INSTANCE_NAME = "client";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setCacheConfiguration(
            new CacheConfiguration()
                .setName(CACHE_NAME)
                .setBackups(1)
        );

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(
                    new TcpDiscoveryVmIpFinder(true)
                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
                )
        );

        cfg.setCommunicationSpi(new MessageCountingCommunicationSpi());

        if (igniteInstanceName.equals(CLIENT_INSTANCE_NAME))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        MessageCountingCommunicationSpi.resetDeploymentRequestCounter();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks, that after client's reconnect to cluster will be redeployed from client node.
     *
     * @throws Exception if test failed.
     */
    public void testAfterClientDisconnect() throws Exception {
        ClassLoader extClsLdr = new GridTestExternalClassLoader(new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

        assertFalse(classFound(getClass().getClassLoader(), PREDICATE_CLASSNAME));

        Class predCls = extClsLdr.loadClass(PREDICATE_CLASSNAME);

        startGrid(0);

        Ignite client = startGrid(CLIENT_INSTANCE_NAME);

        client.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, String> cache = client.getOrCreateCache(CACHE_NAME);

        cache.put(1, "foo");

        invokeScanQueryAndStopClient(client, predCls);

        MessageCountingCommunicationSpi.resetDeploymentRequestCounter();

        client = startGrid(CLIENT_INSTANCE_NAME);

        invokeScanQueryAndStopClient(client, predCls);
    }

    /**
     * @param client ignite instance.
     * @param predCls loaded predicate class.
     * @throws Exception if failed.
     */
    private void invokeScanQueryAndStopClient(Ignite client, Class predCls) throws Exception {
        IgniteCache<Integer, String> cache = client.getOrCreateCache(CACHE_NAME);

        assertEquals("Invalid number of sent grid deployment requests", 0, MessageCountingCommunicationSpi.deploymentRequestCount());

        assertFalse(PREDICATE_CLASSNAME + " mustn't be cached! ", igniteUtilsCachedClasses().contains(PREDICATE_CLASSNAME));

        cache.query(new ScanQuery<>((IgniteBiPredicate<Integer, String>)predCls.newInstance())).getAll();

        // first request is GridDeployment.java 716 and second is GridDeployment.java 501
        assertEquals("Invalid number of sent grid deployment requests", 2, MessageCountingCommunicationSpi.deploymentRequestCount());

        assertTrue(PREDICATE_CLASSNAME + " must be cached! ", igniteUtilsCachedClasses().contains(PREDICATE_CLASSNAME));

        client.close();

        assertFalse(PREDICATE_CLASSNAME + " mustn't be cached! ", igniteUtilsCachedClasses().contains(PREDICATE_CLASSNAME));
    }

    /**
     * @return All class names cached in IgniteUtils.classCache field
     * @throws Exception if something wrong.
     */
    private Set<String> igniteUtilsCachedClasses() throws Exception {
        Field f = IgniteUtils.class.getDeclaredField("classCache");

        f.setAccessible(true);

        ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>> map =
            (ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>>)f.get(null);

        return map.values().stream().flatMap(x -> x.keySet().stream()).collect(Collectors.toSet());
    }

    /**
     * @param clsLdr classloader.
     * @param name classname.
     * @return true if class loaded by classloader, false if class not found.
     */
    private boolean classFound(ClassLoader clsLdr, String name) {
        try {
            clsLdr.loadClass(name);

            return true;
        }
        catch (ClassNotFoundException e) {
            return false;
        }
    }

    /** */
    private static class MessageCountingCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private static final AtomicInteger reqCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackClosure
        ) throws IgniteSpiException {
            if (msg instanceof GridIoMessage && isDeploymentRequestMessage((GridIoMessage)msg))
                reqCnt.incrementAndGet();

            super.sendMessage(node, msg, ackClosure);
        }

        /**
         * @return Number of deployment requests.
         */
        public static int deploymentRequestCount() {
            return reqCnt.get();
        }

        /**
         * Reset request counter.
         */
        public static void resetDeploymentRequestCounter() {
            reqCnt.set(0);
        }

        /**
         * Checks if it is a p2p deployment request message with test predicate resource.
         *
         * @param msg Message to check.
         * @return {@code True} if this is a p2p message.
         */
        private boolean isDeploymentRequestMessage(GridIoMessage msg) {
            try {
                if (msg.message() instanceof GridDeploymentRequest) {
                    // rsrcName getter have only default access level. So, use reflection for access to field value.
                    GridDeploymentRequest req = (GridDeploymentRequest)msg.message();

                    Field f = GridDeploymentRequest.class.getDeclaredField("rsrcName");

                    f.setAccessible(true);

                    return f.get(req).equals(TEST_PREDICATE_RESOURCE_NAME);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            return false;
        }
    }
}
