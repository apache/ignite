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

package org.apache.ignite.internal;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.http.GridEmbeddedHttpServer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.UrlResource;

import static org.apache.ignite.IgniteState.STARTED;
import static org.apache.ignite.IgniteState.STOPPED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Tests for {@link org.apache.ignite.Ignition}.
 * @see GridFactoryVmShutdownTest
 */
@SuppressWarnings("UnusedDeclaration")
@GridCommonTest(group = "NonDistributed Kernal Self")
public class GridFactorySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final AtomicInteger cnt = new AtomicInteger();

    /** Concurrency. */
    private static final int CONCURRENCY = 10;

    /** */
    private static final String CUSTOM_CFG_PATH =
        "modules/core/src/test/config/factory/custom-grid-name-spring-test.xml";

    /**
     *
     */
    public GridFactorySelfTest() {
        super(false);

        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(GridFactorySelfTest.class));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cnt.set(0);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIgnitionStartDefault() throws Exception {
        try (Ignite ignite = Ignition.start()) {
            log.info("Started1: " + ignite.name());

            try {
                Ignition.start();

                fail();
            }
            catch (IgniteException ignored) {
                // No-op.
            }
        }

        try (Ignite ignite = Ignition.start()) {
            log.info("Started2: " + ignite.name());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartFabricDefault() throws Exception {
        try (Ignite ignite = Ignition.start("config/fabric/default-config.xml")) {
            log.info("Started: " + ignite.name());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartDefault() throws Exception {
        try (Ignite ignite = Ignition.start("config/default-config.xml")) {
            log.info("Started: " + ignite.name());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartGridWithConfigUrlString() throws Exception {
        GridEmbeddedHttpServer srv = null;
        String gridName = "grid_with_url_config";

        try {
            srv = GridEmbeddedHttpServer.startHttpServer().withFileDownloadingHandler(null,
                GridTestUtils.resolveIgnitePath("/modules/core/src/test/config/default-spring-url-testing.xml"));

            Ignite ignite = G.start(srv.getBaseUrl());

            assert gridName.equals(ignite.name()) : "Unexpected grid name: " + ignite.name();
        }
        finally {
            if (srv != null)
                srv.stop(1);

            G.stop(gridName, false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartGridWithConfigUrl() throws Exception {
        GridEmbeddedHttpServer srv = null;
        String gridName = "grid_with_url_config";

        try {
            srv = GridEmbeddedHttpServer.startHttpServer().withFileDownloadingHandler(null,
                GridTestUtils.resolveIgnitePath("modules/core/src/test/config/default-spring-url-testing.xml"));

            Ignite ignite = G.start(new URL(srv.getBaseUrl()));

            assert gridName.equals(ignite.name()) : "Unexpected grid name: " + ignite.name();
        }
        finally {
            if (srv != null)
                srv.stop(1);

            G.stop(gridName, false);
        }
    }

    /**
     * Tests default grid
     */
    public void testDefaultGridGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration(null);

        try (Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);

                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
            }

            Ignite ignite2 = Ignition.getOrStart(cfg);

            assertEquals("Must return same instance", ignite, ignite2);
        }

        assertTrue(G.allGrids().isEmpty());
    }

    /**
     * Tests named grid
     */
    public void testNamedGridGetOrStart() throws Exception {
        IgniteConfiguration cfg = getConfiguration("test");
        try (Ignite ignite = Ignition.getOrStart(cfg)) {
            try {
                Ignition.start(cfg);

                fail("Expected exception after grid started");
            }
            catch (IgniteException ignored) {
                // No-op.
            }

            Ignite ignite2 = Ignition.getOrStart(cfg);

            assertEquals("Must return same instance", ignite, ignite2);
        }

        assertTrue(G.allGrids().isEmpty());
    }

    /**
     * Tests concurrent grid initialization
     */
    public void testConcurrentGridGetOrStartCon() throws Exception {
        final IgniteConfiguration cfg = getConfiguration(null);

        final AtomicReference<Ignite> ref = new AtomicReference<>();

        try {
            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    // must return same instance in each thread

                    try {
                        Ignite ignite = Ignition.getOrStart(cfg);

                        boolean set = ref.compareAndSet(null, ignite);

                        if (!set)
                            assertEquals(ref.get(), ignite);
                    }
                    catch (IgniteException e) {
                        throw new RuntimeException("Ignite error", e);
                    }
                }
            }, CONCURRENCY, "GridCreatorThread");
        }
        catch (Exception ignored) {
            fail("Exception is not expected");
        }

        G.stopAll(true);

        assertTrue(G.allGrids().isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBeansNullGridName() throws Exception {
        checkLifecycleBeans(null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBeansNotNullGridName() throws Exception {
        checkLifecycleBeans("testGrid");
    }

    /**
     * @param gridName Grid name.
     * @throws Exception If test failed.
     */
    private void checkLifecycleBeans(@Nullable String gridName) throws Exception {
        TestLifecycleBean bean1 = new TestLifecycleBean();
        TestLifecycleBean bean2 = new TestLifecycleBean();

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLifecycleBeans(bean1, bean2);
        cfg.setGridName(gridName);

        cfg.setConnectorConfiguration(null);

        try (Ignite g = IgniteSpring.start(cfg, new GenericApplicationContext())) {
            bean1.checkState(gridName, true);
            bean2.checkState(gridName, true);
        }

        bean1.checkState(gridName, false);
        bean2.checkState(gridName, false);

        checkLifecycleBean(bean1, gridName);
        checkLifecycleBean(bean2, gridName);
    }

    /**
     * @param bean Bean to check.
     * @param gridName Grid name to check for.
     */
    private void checkLifecycleBean(TestLifecycleBean bean, String gridName) {
        bean.checkErrors();

        List<LifecycleEventType> evts = bean.getLifecycleEvents();

        List<String> gridNames = bean.getGridNames();

        assert evts.get(0) == LifecycleEventType.BEFORE_NODE_START : "Invalid lifecycle event: " + evts.get(0);
        assert evts.get(1) == LifecycleEventType.AFTER_NODE_START : "Invalid lifecycle event: " + evts.get(1);
        assert evts.get(2) == LifecycleEventType.BEFORE_NODE_STOP : "Invalid lifecycle event: " + evts.get(2);
        assert evts.get(3) == LifecycleEventType.AFTER_NODE_STOP : "Invalid lifecycle event: " + evts.get(3);

        checkGridNameEquals(gridNames.get(0), gridName);
        checkGridNameEquals(gridNames.get(1), gridName);
        checkGridNameEquals(gridNames.get(2), gridName);
        checkGridNameEquals(gridNames.get(3), gridName);
    }

    /**
     * @param n1 First name.
     * @param n2 Second name.
     */
    private void checkGridNameEquals(String n1, String n2) {
        if (n1 == null) {
            assert n2 == null;

            return;
        }

        assert n1.equals(n2) : "Invalid grid names [name1=" + n1 + ", name2=" + n2 + ']';
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    public void testStartMultipleGridsFromSpring() throws Exception {
        File cfgFile =
            GridTestUtils.resolveIgnitePath(GridTestProperties.getProperty("loader.self.multipletest.config"));

        assert cfgFile != null;

        String path = cfgFile.getAbsolutePath();

        info("Loading Grid from configuration file: " + path);

        final GridTuple<IgniteState> gridState1 = F.t(null);
        final GridTuple<IgniteState> gridState2 = F.t(null);

        final Object mux = new Object();

        IgnitionListener factoryLsnr = new IgnitionListener() {
            @Override public void onStateChange(String name, IgniteState state) {
                synchronized (mux) {
                    if ("grid-factory-test-1".equals(name))
                        gridState1.set(state);
                    else if ("grid-factory-test-2".equals(name))
                        gridState2.set(state);
                }
            }
        };

        G.addListener(factoryLsnr);

        G.start(path);

        assert G.ignite("grid-factory-test-1") != null;
        assert G.ignite("grid-factory-test-2") != null;

        synchronized (mux) {
            assert gridState1.get() == STARTED :
                "Invalid grid state [expected=" + STARTED + ", returned=" + gridState1 + ']';
            assert gridState2.get() == STARTED :
                "Invalid grid state [expected=" + STARTED + ", returned=" + gridState2 + ']';
        }

        G.stop("grid-factory-test-1", true);
        G.stop("grid-factory-test-2", true);

        synchronized (mux) {
            assert gridState1.get() == STOPPED :
                "Invalid grid state [expected=" + STOPPED + ", returned=" + gridState1 + ']';
            assert gridState2.get() == STOPPED :
                "Invalid grid state [expected=" + STOPPED + ", returned=" + gridState2 + ']';
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartMultipleDefaultGrids() throws Exception {
        try {
            multithreaded(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        try {
                            IgniteConfiguration cfg = new IgniteConfiguration();

                            cfg.setConnectorConfiguration(null);

                            G.start(cfg);
                        }
                        catch (Throwable t) {
                            error("Caught exception while starting grid.", t);
                        }

                        info("Thread finished.");

                        return null;
                    }
                },
                5,
                "grid-starter"
            );

            assert G.allGrids().size() == 1;

            assert G.ignite() != null;
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartMultipleNonDefaultGrids() throws Exception {
        try {
            multithreaded(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        try {
                            IgniteConfiguration cfg = new IgniteConfiguration();

                            cfg.setGridName("TEST_NAME");
                            cfg.setConnectorConfiguration(null);

                            G.start(cfg);
                        }
                        catch (Throwable t) {
                            error("Caught exception while starting grid.", t);
                        }

                        info("Thread finished.");

                        return null;
                    }
                },
                5,
                "grid-starter"
            );

            assert G.allGrids().size() == 1;

            assert G.ignite("TEST_NAME") != null;
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartStop() throws Exception {
        checkConcurrentStartStop("TEST_NAME");
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentStartStopDefaultGrid() throws Exception {
        checkConcurrentStartStop(null);
    }

    /**
     * @param gridName Grid name ({@code null} for default grid).
     * @throws Exception If failed.
     */
    private void checkConcurrentStartStop(@Nullable final String gridName) throws Exception {
        final AtomicInteger startedCnt = new AtomicInteger();
        final AtomicInteger stoppedCnt = new AtomicInteger();

        IgnitionListener lsnr = new IgnitionListener() {
            @SuppressWarnings("StringEquality")
            @Override public void onStateChange(@Nullable String name, IgniteState state) {
                assert name == gridName;

                info("On state change fired: " + state);

                if (state == STARTED)
                    startedCnt.incrementAndGet();
                else {
                    assert state == STOPPED : "Unexpected state: " + state;

                    stoppedCnt.incrementAndGet();
                }
            }
        };

        G.addListener(lsnr);

        try {
            final int iterCnt = 3;

            multithreaded(
                new Callable<Object>() {
                    @Nullable @Override public Object call() throws Exception {
                        for (int i = 0; i < iterCnt; i++) {
                            try {
                                IgniteConfiguration cfg = getConfiguration(gridName);

                                G.start(cfg);
                            }
                            catch (Exception e) {
                                String msg = e.getMessage();

                                if (msg != null &&
                                    (msg.contains("Default Ignite instance has already been started.") ||
                                    msg.contains("Ignite instance with this name has already been started:")))
                                    info("Caught expected exception: " + msg);
                                else
                                    throw e; // Unexpected exception.
                            }
                            finally {
                                stopGrid(gridName);
                            }
                        }

                        info("Thread finished.");

                        return null;
                    }
                },
                5,
                "tester"
            );

            assert G.allGrids().isEmpty();

            assert startedCnt.get() == iterCnt;
            assert stoppedCnt.get() == iterCnt;
        }
        finally {
            G.removeListener(lsnr);

            G.stopAll(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridStartRollback() throws Exception {
        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    IgniteConfiguration cfg = new IgniteConfiguration();

                    cfg.setConnectorConfiguration(null);

                    cfg.setDiscoverySpi(new TcpDiscoverySpi() {
                        @Override public void spiStart(String gridName) throws IgniteSpiException {
                            throw new IgniteSpiException("This SPI will never start.");
                        }
                    });

                    G.start(cfg);

                    info("Thread finished.");

                    return null;
                }
            },
            IgniteException.class,
            null
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void disabledTestStartSingleInstanceSpi() throws Exception {
        IgniteConfiguration cfg1 = getConfiguration();
        IgniteConfiguration cfg2 = getConfiguration();

        cfg1.setCollisionSpi(new TestSingleInstancesCollisionSpi());
        cfg2.setCollisionSpi(new TestSingleInstancesCollisionSpi());

        G.start(cfg1);

        assert G.state(cfg1.getGridName()) == STARTED;
        assert G.state(getTestGridName() + '1') == STOPPED;

        G.stop(cfg1.getGridName(), false);

        assert G.state(cfg1.getGridName()) == STOPPED;
        assert G.state(getTestGridName() + '1') == STOPPED;

        cfg2.setGridName(getTestGridName() + '1');

        G.start(cfg2);

        assert G.state(cfg1.getGridName()) == STOPPED;
        assert G.state(getTestGridName() + '1') == STARTED;

        G.stop(getTestGridName() + '1', false);

        assert G.state(cfg1.getGridName()) == STOPPED;
        assert G.state(getTestGridName() + '1') == STOPPED;

        cfg2.setGridName(getTestGridName() + '1');

        G.start(cfg2);

        assert G.state(getTestGridName() + '1') == STARTED;
        assert G.state(getTestGridName()) == STOPPED;

        G.stop(getTestGridName() + '1', false);
        G.stop(getTestGridName(), false);

        assert G.state(getTestGridName() + '1') == STOPPED;
        assert G.state(getTestGridName()) == STOPPED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartMultipleInstanceSpi() throws Exception {
        IgniteConfiguration cfg1 = getConfiguration();
        IgniteConfiguration cfg2 = getConfiguration();
        IgniteConfiguration cfg3 = getConfiguration();

        cfg1.setCollisionSpi(new TestMultipleInstancesCollisionSpi());
        cfg2.setCollisionSpi(new TestMultipleInstancesCollisionSpi());
        cfg3.setCollisionSpi(new TestMultipleInstancesCollisionSpi());

        cfg2.setGridName(getTestGridName() + '1');

        G.start(cfg2);

        G.start(cfg1);

        cfg3.setGridName(getTestGridName() + '2');

        G.start(cfg3);

        assert G.state(cfg1.getGridName()) == STARTED;
        assert G.state(getTestGridName() + '1') == STARTED;
        assert G.state(getTestGridName() + '2') == STARTED;

        G.stop(getTestGridName() + '2', false);
        G.stop(cfg1.getGridName(), false);
        G.stop(getTestGridName() + '1', false);

        assert G.state(cfg1.getGridName()) == STOPPED;
        assert G.state(getTestGridName() + '1') == STOPPED;
        assert G.state(getTestGridName() + '2') == STOPPED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadBean() throws Exception {
        final String path = "modules/spring/src/test/java/org/apache/ignite/internal/cache.xml";

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Ignition.loadSpringBean(path, "wrongName");

                    return null;
                }
            },
            IgniteException.class,
            null
        );

        CacheConfiguration cfg = Ignition.loadSpringBean(path, "cache-configuration");

        assertEquals("TestDynamicCache", cfg.getName());
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTest() throws Exception {
        G.stopAll(false);
    }

    /** */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class TestMultipleInstancesCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** Grid logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException {
            // Start SPI start stopwatch.
            startStopwatch();

            // Ack start.
            if (log.isInfoEnabled())
                log.info(startInfo());
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // Ack stop.
            if (log.isInfoEnabled())
                log.info(stopInfo());
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }
    }

    /**
     * DO NOT CHANGE MULTIPLE INSTANCES SUPPORT.
     * This test might be working on distributed environment.
     */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class TestSingleInstancesCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** Grid logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException {
            // Start SPI start stopwatch.
            startStopwatch();

            // Ack start.
            if (log.isInfoEnabled())
                log.info(startInfo());
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // Ack stop.
            if (log.isInfoEnabled())
                log.info(stopInfo());
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }
    }

    /**
     * Lifecycle bean for testing.
     */
    private static class TestLifecycleBean implements LifecycleBean {
        /** Grid logger. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @SpringApplicationContextResource
        private ApplicationContext appCtx;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Lifecycle events. */
        private final List<LifecycleEventType> evts = new ArrayList<>();

        /** Grid names. */
        private final List<String> gridNames = new ArrayList<>();

        /** */
        private final AtomicReference<Throwable> err = new AtomicReference<>();

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            evts.add(evt);

            gridNames.add(ignite.name());

            try {
                checkState(ignite.name(),
                    evt == LifecycleEventType.AFTER_NODE_START || evt == LifecycleEventType.BEFORE_NODE_STOP);
            }
            catch (Throwable e) {
                log.error("Lifecycle bean failed state check: " + this, e);

                err.compareAndSet(null, e);
            }
        }

        /**
         * Checks state of the bean.
         *
         * @param gridName Grid name.
         * @param exec Try to execute something on the grid.
         */
        void checkState(String gridName, boolean exec) {
            assert log != null;
            assert appCtx != null;

            assert F.eq(gridName, ignite.name());

            if (exec)
                // Execute any grid method.
                G.ignite(gridName).events().localQuery(F.<Event>alwaysTrue());
        }

        /**
         * Gets ordered list of lifecycle events.
         *
         * @return Ordered list of lifecycle events.
         */
        List<LifecycleEventType> getLifecycleEvents() {
            return evts;
        }

        /**
         * Gets ordered list of grid names.
         *
         * @return Ordered list of grid names.
         */
        List<String> getGridNames() {
            return gridNames;
        }

        /**
         *
         */
        void checkErrors() {
            if (err.get() != null)
                fail("Exception has been caught by listener: " + err.get().getMessage());
        }
    }

    /**
     * Gets Spring application context by given path.
     *
     * @param path Spring application context configuration path.
     * @return Spring application context.
     * @throws IgniteCheckedException If given path or xml-configuration at this path is invalid.
     */
    private GenericApplicationContext getSpringContext(String path) throws IgniteCheckedException {
        try {
            GenericApplicationContext ctx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(ctx).loadBeanDefinitions(new UrlResource(U.resolveIgniteUrl(path)));

            ctx.refresh();

            return ctx;
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate Spring XML application context: " + e.getMessage(), e);
        }
    }

    /**
     * Gets test Spring application context with single {@link StringBuilder} bean
     * with name "myBean" and value "Test string".
     *
     * @return Spring application context.
     */
    private ApplicationContext getTestApplicationContext() {
        AbstractBeanDefinition def = new GenericBeanDefinition();

        def.setBeanClass(StringBuilder.class);

        ConstructorArgumentValues args = new ConstructorArgumentValues();
        args.addGenericArgumentValue("Test string");

        def.setConstructorArgumentValues(args);

        GenericApplicationContext ctx = new GenericApplicationContext();

        ctx.registerBeanDefinition("myBean", def);

        return ctx;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopCancel() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setConnectorConfiguration(null);

        Ignite ignite = G.start(cfg);

        ignite.compute().execute(TestTask.class, null);

        G.stop(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConfigInClassPath() throws Exception {
        try (Ignite ignite = Ignition.start("config/ignite-test-config.xml")) {
            assert "config-in-classpath".equals(ignite.name());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCurrentIgnite() throws Exception {
        final String LEFT = "LEFT";
        final String RIGHT = "RIGHT";

        try {
            Ignite iLEFT = startGrid(LEFT);
            Ignite iRIGHT = startGrid(RIGHT);

            waitForDiscovery(iLEFT, iRIGHT);

            iLEFT.compute(iLEFT.cluster().forRemotes()).run(new IgniteRunnable() {
                @Override public void run() {
                    assert Ignition.localIgnite().name().equals(RIGHT);
                }
            });

            iRIGHT.compute(iRIGHT.cluster().forRemotes()).run(new IgniteRunnable() {
                @Override public void run() {
                    assert Ignition.localIgnite().name().equals(LEFT);
                }
            });
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRepeatingStart() throws Exception {
        try {
            IgniteConfiguration c = getConfiguration("1");

            startGrid("1", c);

            assert ((TcpDiscoverySpi)c.getDiscoverySpi()).started();

            try {
                startGrid("2", c);

                fail("Should not be able to start grid using same configuration instance.");
            }
            catch (Exception e) {
                info("Caught expected exception: " + e);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test task.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) {
            return F.asSet(new TestJob());
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Test job.
     */
    private static class TestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @SuppressWarnings("StatementWithEmptyBody")
        @Override public Object execute() {
            long start = System.currentTimeMillis();

            while (System.currentTimeMillis() - start < 3000);

            return null;
        }
    }
}
