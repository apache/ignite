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

import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DeploymentEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_TASK_UNDEPLOYED;

/**
 * The test does the following:
 *
 * 1. The test should execute a task in SHARED_DEPLOY mode, restart a node with new version and make sure that a
 *      new class loader is created on remote node.
 * 2. The test should execute a task in SHARED_DEPLOY mode, restart a node with same version and make sure
 *      that the same class loader is created on remote node.
 * 3. The test should execute a task in SHARED_UNDEPLOY mode, restart a node with same version and
 *      make sure that a new class loader is created on remote node.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ObjectEquality", "unchecked"})
public class GridP2PUserVersionChangeSelfTest extends GridCommonAbstractTest {
    /** Current deployment mode. */
    private DeploymentMode depMode;

    /** Test task class name. */
    private static final String TEST_TASK_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** Test resource class name. */
    private static final String TEST_RCRS_NAME = "org.apache.ignite.tests.p2p.TestUserResource";

    /** IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    public GridP2PUserVersionChangeSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 1000;
    }

    /**
     * @return Timeout for condition waits.
     */
    private long getConditionTimeout() {
        return getTestTimeout() > 10000 ? getTestTimeout() - 10000 : getTestTimeout();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);
        cfg.setNetworkTimeout(10000);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        if (gridName.contains("testCacheRedeployVersionChangeContinuousMode")) {
            CacheConfiguration cacheCfg = new CacheConfiguration();

            cacheCfg.setCacheMode(CacheMode.REPLICATED);

            cfg.setCacheConfiguration(cacheCfg);
        }
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testRedeployVersionChangeContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        checkRedeployVersionChange();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testRedeployVersionChangeSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        checkRedeployVersionChange();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkRedeployVersionChange() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(
                new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) },
                Collections.singletonMap("META-INF/ignite.xml", makeUserVersion("1").getBytes()));

            Class task1 = ldr.loadClass(TEST_TASK_NAME);

            final CountDownLatch undeployed = new CountDownLatch(1);

            ignite2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_TASK_UNDEPLOYED &&
                        ((DeploymentEvent) evt).alias().equals(TEST_TASK_NAME))
                        undeployed.countDown();

                    return true;
                }
            }, EVT_TASK_UNDEPLOYED);

            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            stopGrid(1);

            ldr.setResourceMap(Collections.singletonMap("META-INF/ignite.xml", makeUserVersion("2").getBytes()));

            ignite1 = startGrid(1);

            Integer res2 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            assert !res1.equals(res2);

            // Allow P2P timeout to expire.
            assert undeployed.await(30000, MILLISECONDS);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRedeployOnNodeRestartContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(
                new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) });

            Class task1 = ldr.loadClass(TEST_TASK_NAME);

            final CountDownLatch undeployed = new CountDownLatch(1);

            ignite2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_TASK_UNDEPLOYED &&
                        ((DeploymentEvent) evt).alias().equals(TEST_TASK_NAME))
                        undeployed.countDown();

                    return true;
                }
            }, EVT_TASK_UNDEPLOYED);

            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            stopGrid(1);

            ignite1 = startGrid(1);

            Integer res2 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            assert !undeployed.await(3000, MILLISECONDS);

            assert res1.equals(res2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRedeployOnNodeRestartSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(
                new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) });

            Class task1 = ldr.loadClass(TEST_TASK_NAME);

            final CountDownLatch undeployed = new CountDownLatch(1);

            ignite2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_TASK_UNDEPLOYED &&
                        ((DeploymentEvent) evt).alias().equals(TEST_TASK_NAME))
                        undeployed.countDown();

                    return true;
                }
            }, EVT_TASK_UNDEPLOYED);

            final CountDownLatch discoLatch = new CountDownLatch(1);

            ignite2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (evt.type() == EVT_NODE_LEFT)
                        discoLatch.countDown();

                    return true;
                }
            }, EVT_NODE_LEFT);

            Integer res1 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            stopGrid(1);

            assert discoLatch.await(1000, MILLISECONDS);

            assert undeployed.await(1000, MILLISECONDS);

            ignite1 = startGrid(1);

            Integer res2 = (Integer)ignite1.compute().execute(task1, ignite2.cluster().localNode().id());

            assert !res1.equals(res2);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * TODO: IGNITE-604.
     *
     * @throws Exception If failed.
     */
    public void testCacheRedeployVersionChangeContinuousMode() throws Exception {
        // Build execution timeout if try to run test on TC.
        fail("https://issues.apache.org/jira/browse/IGNITE-604");
        
        depMode = DeploymentMode.CONTINUOUS;

        try {
            Ignite ignite1 = startGrid("testCacheRedeployVersionChangeContinuousMode1");
            Ignite ignite2 = startGrid("testCacheRedeployVersionChangeContinuousMode2");

            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(
                new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) },
                Collections.singletonMap("META-INF/ignite.xml", makeUserVersion("1").getBytes()));

            Class rcrsCls = ldr.loadClass(TEST_RCRS_NAME);

            IgniteCache<Long, Object> cache1 = ignite1.cache(null);

            assertNotNull(cache1);

            cache1.put(1L, rcrsCls.newInstance());

            final IgniteCache<Long, Object> cache2 = ignite2.cache(null);

            assertNotNull(cache2);

            // The entry should propagate to grid2, because the
            // cache is REPLICATED. This happens asynchronously, we
            // need to use condition wait.
            assert GridTestUtils.waitForCondition(new PAX() {
                @Override public boolean applyx() {
                    return cache2.get(1L) != null;
                }
            }, getConditionTimeout());

            stopGrid("testCacheRedeployVersionChangeContinuousMode1");

            // Increase the user version of the test class.
            ldr.setResourceMap(Collections.singletonMap("META-INF/ignite.xml", makeUserVersion("2").getBytes()));

            ignite1 = startGrid("testCacheRedeployVersionChangeContinuousMode1");

            cache1 = ignite1.cache(null);

            assertNotNull(cache1);

            // Put an entry with a new user version.
            cache1.put(2L, rcrsCls.newInstance());

            // At this point, old version of test resource should be undeployed
            // and removed from cache asynchronously.
            assert GridTestUtils.waitForCondition(new PAX() {
                @Override public boolean applyx() {
                    return cache2.get(1L) == null;
                }
            }, getConditionTimeout()) : "2nd condition failed [entries1=" + toSet(cache1.iterator()) +
                ", entries2=" + toSet(cache2.iterator()) + ']';
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private <K, V> Set<Cache.Entry<K, V>> toSet(Iterator<Cache.Entry<K, V>> iter){
        Set<Cache.Entry<K, V>> set = new HashSet<>();

        while (iter.hasNext())
            set.add(iter.next());

        return set;
    }

    /**
     * Creates content of META-INF/ignite.xml for specified user version.
     *
     * @param userVer Version to create.
     * @return content of META-INF/ignite.xml.
     */
    private String makeUserVersion(String userVer) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " +
            "<beans xmlns=\"http://www.springframework.org/schema/beans\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xmlns:util=\"http://www.springframework.org/schema/util\" " +
            "xsi:schemaLocation=\"http://www.springframework.org/schema/beans " +
            "http://www.springframework.org/schema/beans/spring-beans.xsd " +
            "http://www.springframework.org/schema/util " +
            "http://www.springframework.org/schema/util/spring-util.xsd\"> " +
            "<bean id=\"userVersion\" class=\"java.lang.String\"><constructor-arg value=\"" + userVer + "\"/></bean> " +
            "</beans>";
    }
}