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

package org.apache.ignite.testframework.junits.spi;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchange;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig.ConfigType;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.lang.IgniteProductVersion.fromString;

/**
 * Base SPI test class.
 * @param <T> SPI implementation class.
 */
@SuppressWarnings({"JUnitTestCaseWithNonTrivialConstructors"})
public abstract class GridSpiAbstractTest<T extends IgniteSpi> extends GridAbstractTest {
    /** */
    private static final IgniteProductVersion VERSION = fromString("99.99.99");

    /** */
    private static final Map<Class<?>, TestData<?>> tests = new ConcurrentHashMap<>();

    /** */
    private final boolean autoStart;

    /** Original context classloader. */
    private ClassLoader cl;

    /** */
    protected GridSpiAbstractTest() {
        super(false);

        autoStart = true;
    }

    /**
     * @param autoStart Start automatically.
     */
    protected GridSpiAbstractTest(boolean autoStart) {
        super(false);

        this.autoStart = autoStart;
    }

    /** {@inheritDoc} */
    @Override protected final boolean isJunitFrameworkClass() {
        return true;
    }

    /**
     * @return Test data.
     */
    @SuppressWarnings({"unchecked"})
    protected TestData<T> getTestData() throws IgniteCheckedException {
        TestData<T> data = (TestData<T>)tests.get(getClass());

        if (data == null)
            tests.put(getClass(), data = new TestData<>());

        return data;
    }

    /**
     * @return Allow all permission security set.
     */
    private static SecurityPermissionSet getAllPermissionSet() {
        return new SecurityPermissionSetImpl();
    }

    /**
     * @throws Exception If failed.
     */
    private void resetTestData() throws Exception {
        tests.put(getClass(), new TestData<T>());
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected final void setUp() throws Exception {
        // Need to change classloader here, although it also handled in the parent class
        // the current test initialisation procedure doesn't allow us to setUp the parent first.
        cl = Thread.currentThread().getContextClassLoader();

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        TestCounters cntrs = getTestCounters();

        if (cntrs.isReset())
            cntrs.reset();

        cntrs.incrementStarted();

        if (autoStart && isFirstTest()) {
            GridSpiTest spiTest = GridTestUtils.getAnnotation(getClass(), GridSpiTest.class);

            assert spiTest != null;

            beforeSpiStarted();

            if (spiTest.trigger())
                spiStart();

            info("==== Started spi test [test=" + getClass().getSimpleName() + "] ====");
        }

        super.setUp();
    }

    /**
     * @throws Exception If failed.
     */
    protected void beforeSpiStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected final IgniteTestResources getTestResources() throws IgniteCheckedException {
        return getTestData().getTestResources();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked", "CastToIncompatibleInterface", "InstanceofIncompatibleInterface"})
    protected final void spiStart() throws Exception {
        GridSpiTest spiTest = GridTestUtils.getAnnotation(getClass(), GridSpiTest.class);

        assert spiTest != null;

        T spi = (T)spiTest.spi().newInstance();

        // Set spi into test data.
        getTestData().setSpi(spi);

        if (!(spi instanceof DiscoverySpi)) {
            if (spiTest.triggerDiscovery())
                configureDiscovery(spiTest);
        }
        else
            getTestData().setDiscoverySpi((DiscoverySpi)spi);

        getTestResources().inject(spi);

        spiConfigure(spi);

        Map<String, Object> attrs = spi.getNodeAttributes();

        // Set up SPI class name and SPI version.
        Map<String, Serializable> spiAttrs = initSpiClassAndVersionAttributes(spi);

        if (attrs != null)
            getTestData().getAttributes().putAll(attrs);

        if (spiAttrs != null)
            getTestData().getAttributes().putAll(spiAttrs);

        Map<String, Serializable> nodeAttrs = getNodeAttributes();

        if (nodeAttrs != null)
            getTestData().getAttributes().putAll(nodeAttrs);

        DiscoverySpi discoSpi = getTestData().getDiscoverySpi();

        if (spiTest.triggerDiscovery() && !getTestData().isDiscoveryTest()) {
            getTestData().getAttributes().putAll(
                initSpiClassAndVersionAttributes(discoSpi));

            // Set all local node attributes into discovery SPI.
            discoSpi.setNodeAttributes(getTestData().getAttributes(), VERSION);

            discoSpi.setMetricsProvider(createMetricsProvider());

            discoSpi.setDataExchange(new DiscoverySpiDataExchange() {
                @Override public Map<Integer, Serializable> collect(UUID nodeId) {
                    return new HashMap<>();
                }

                @Override public void onExchange(UUID joiningNodeId, UUID nodeId, Map<Integer, Serializable> data) {
                }
            });

            try {
                spiStart(discoSpi);
            }
            catch (Exception e) {
                spiStop(discoSpi);

                throw e;
            }
        }

        if (spi instanceof DiscoverySpi) {
            getTestData().getAttributes().putAll(initSpiClassAndVersionAttributes(spi));

            ((DiscoverySpi)spi).setNodeAttributes(getTestData().getAttributes(), VERSION);

            ((DiscoverySpi)spi).setMetricsProvider(createMetricsProvider());
        }

        try {
            spiStart(spi);
        }
        catch (Exception e) {
            spiStop(spi);

            if (discoSpi != null && !discoSpi.equals(spi))
                spiStop(discoSpi);

            afterSpiStopped();

            throw e;
        }

        // Initialize SPI context.
        getTestData().setSpiContext(initSpiContext());

        // Initialize discovery SPI only once.
        if (discoSpi != null && !discoSpi.equals(spi))
            discoSpi.onContextInitialized(getSpiContext());

        spi.onContextInitialized(getTestData().getSpiContext());
    }

    /**
     * @return SPI context.
     * @throws Exception If anything failed.
     */
    protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext spiCtx = new GridSpiTestContext();

        if (getTestData().getDiscoverySpi() != null) {
            spiCtx.setLocalNode(getTestData().getDiscoverySpi().getLocalNode());

            for (ClusterNode node : getTestData().getDiscoverySpi().getRemoteNodes())
                spiCtx.addNode(node);
        }
        else {
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            spiCtx.setLocalNode(node);

            if (getSpi() != null) {
                // Set up SPI class name and SPI version.
                Map<String, Serializable> attrs = initSpiClassAndVersionAttributes(getSpi());

                for (Map.Entry<String, Serializable> entry: attrs.entrySet())
                    node.addAttribute(entry.getKey(), entry.getValue());
            }
        }

        return spiCtx;
    }

    /**
     * @param spi SPI to create attributes for.
     * @return Map of attributes.
     */
    private Map<String, Serializable> initSpiClassAndVersionAttributes(IgniteSpi spi) {
        Map<String, Serializable> attrs = new HashMap<>();

        attrs.put(U.spiAttribute(spi, IgniteNodeAttributes.ATTR_SPI_CLASS), spi.getClass().getName());

        return attrs;
    }

    /**
     * @return SPI context.
     * @throws Exception If anything failed.
     */
    protected final GridSpiTestContext getSpiContext() throws Exception {
        return getTestData().getSpiContext();
    }

    /**
     * @param spiTest SPI test annotation.
     * @throws Exception If anything failed.
     */
    private void configureDiscovery(GridSpiTest spiTest) throws Exception {
        DiscoverySpi discoSpi = spiTest.discoverySpi().newInstance();

        if (discoSpi instanceof TcpDiscoverySpi) {
            TcpDiscoverySpi tcpDisco = (TcpDiscoverySpi)discoSpi;

            tcpDisco.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        }

        getTestData().setDiscoverySpi(discoSpi);

        getTestResources().inject(discoSpi);

        configure(discoSpi);

        if (discoSpi.getNodeAttributes() != null)
            getTestData().getAttributes().putAll(discoSpi.getNodeAttributes());
    }

    /**
     * Creates metrics provider just for testing purposes. The provider
     * will not return valid node metrics.
     *
     * @return Dummy metrics provider.
     */
    protected DiscoveryMetricsProvider createMetricsProvider() {
        return new DiscoveryMetricsProvider() {
            /** {@inheritDoc} */
            @Override public ClusterMetrics metrics() { return new ClusterMetricsSnapshot(); }

            /** {@inheritDoc} */
            @Override public Map<Integer, CacheMetrics> cacheMetrics() {
                return Collections.emptyMap();
            }
        };
    }

    /**
     * @param spi SPI.
     * @throws Exception If failed.
     */
    protected void spiConfigure(T spi) throws Exception {
        configure(spi);
    }

    /**
     * @param spi SPI.
     * @throws Exception If failed.
     * @throws IllegalAccessException If failed.
     * @throws InvocationTargetException If failed.
     */
    private void configure(IgniteSpi spi) throws Exception {
        // Inject Configuration.
        for (Method m : getClass().getMethods()) {
            GridSpiTestConfig cfg = m.getAnnotation(GridSpiTestConfig.class);

            if (cfg != null) {
                if (getTestData().isDiscoveryTest() ||
                    (cfg.type() != ConfigType.DISCOVERY && !(spi instanceof DiscoverySpi)) ||
                    (cfg.type() != ConfigType.SELF && spi instanceof DiscoverySpi)) {
                    assert m.getName().startsWith("get") : "Test configuration must be a getter [method=" +
                        m.getName() + ']';

                    // Determine getter name.
                    String name = cfg.setterName();

                    if (name == null || name.isEmpty())
                        name = 's' + m.getName().substring(1);

                    Method setter = getMethod(spi.getClass(), name);

                    assert setter != null : "Spi does not have setter for configuration property [spi=" +
                        spi.getClass().getName() + ", config-prop=" + name + ']';

                    // Inject configuration parameter into spi.
                    setter.invoke(spi, m.invoke(this));
                }
            }
        }

        // Our SPI tests should not have the same parameters otherwise they
        // will find each other.
        if (spi instanceof TcpCommunicationSpi)
            ((TcpCommunicationSpi)spi).setLocalPort(GridTestUtils.getNextCommPort(getClass()));

        if (spi instanceof TcpDiscoverySpi) {
            TcpDiscoveryIpFinder ipFinder = ((TcpDiscoverySpi)spi).getIpFinder();

            if (ipFinder instanceof TcpDiscoveryMulticastIpFinder) {
                String mcastAddr = GridTestUtils.getNextMulticastGroup(getClass());

                if (mcastAddr != null && !mcastAddr.isEmpty()) {
                    ((TcpDiscoveryMulticastIpFinder)ipFinder).setMulticastGroup(mcastAddr);
                    ((TcpDiscoveryMulticastIpFinder)ipFinder).setMulticastPort(
                        GridTestUtils.getNextMulticastPort(getClass()));
                }
            }
        }
    }

    /**
     * @param spi SPI.
     * @throws Exception If failed.
     */
    protected void spiStart(IgniteSpi spi) throws Exception {
        U.setWorkDirectory(null, U.getIgniteHome());

        // Start SPI with unique grid name.
        spi.spiStart(getTestGridName());

        info("SPI started [spi=" + spi.getClass() + ']');
    }

    /**
     * @return  Fully initialized and started SPI implementation.
     */
    protected final T getSpi() throws IgniteCheckedException {
        return getTestData().getSpi();
    }

    /**
     * Gets class of the SPI implementation.
     *
     * @return Class of the SPI implementation.
     */
    @SuppressWarnings({"unchecked"})
    protected final Class<? extends T> getSpiClass() {
        GridSpiTest spiTest = GridTestUtils.getAnnotation(getClass(), GridSpiTest.class);

        assert spiTest != null;

        return (Class<? extends T>)spiTest.spi();
    }

    /**
     * @return Node UUID.
     * @throws Exception If failed.
     */
    protected UUID getNodeId() throws Exception {
        return getTestResources().getNodeId();
    }

    /**
     * @return Discovery SPI.
     * @throws Exception If failed.
     */
    @Nullable protected final DiscoverySpi getDiscoverySpi() throws Exception {
        if (getTestData() != null)
            return getTestData().getDiscoverySpi();

        return null;
    }

    /**
     * Override this method for put local node attributes to discovery SPI.
     *
     * @return Always {@code null}.
     */
    @Nullable protected Map<String, Serializable> getNodeAttributes() {
        return null;
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected final void tearDown() throws Exception {
        getTestCounters().incrementStopped();

        boolean wasLast = isLastTest();

        super.tearDown();

        if (autoStart && wasLast) {
            GridSpiTest spiTest = GridTestUtils.getAnnotation(getClass(), GridSpiTest.class);

            assert spiTest != null;

            if (spiTest.trigger()) {
                spiStop();

                afterSpiStopped();
            }

            info("==== Stopped spi test [test=" + getClass().getSimpleName() + "] ====");
        }

        Thread.currentThread().setContextClassLoader(cl);
    }

    /**
     * @throws Exception If failed..
     */
    protected void afterSpiStopped() throws Exception {
        // No-op.
    }

    /**
     * @throws Exception If failed.
     */
    protected final void spiStop() throws Exception {
        TestData<T> testData = getTestData();

        if (testData.getSpi() == null)
            return;

        testData.getSpi().onContextDestroyed();

        spiStop(testData.getSpi());

        GridSpiTest spiTest = GridTestUtils.getAnnotation(getClass(), GridSpiTest.class);

        assert spiTest != null;

        if (!testData.isDiscoveryTest() && spiTest.triggerDiscovery()) {
            testData.getDiscoverySpi().onContextDestroyed();

            spiStop(testData.getDiscoverySpi());
        }

        getTestResources().stopThreads();

        resetTestData();
    }

    /**
     * @param spi SPI.
     * @throws Exception If failed.
     */
    protected void spiStop(IgniteSpi spi) throws Exception {
        spi.spiStop();

        info("SPI stopped [spi=" + spi.getClass().getName() + ']');
    }

    /**
     * @param cls Class.
     * @param name Method name.
     * @return Method.
     */
    @Nullable private Method getMethod(Class<?> cls, String name) {
        for (; !cls.equals(Object.class); cls = cls.getSuperclass()) {
            for (Method m : cls.getMethods()) {
                if (m.getName().equals(name))
                    return m;
            }
        }

        return null;
    }

    /**
     *
     * @param <T> SPI implementation class.
     */
    protected static class TestData<T> {
        /** */
        private T spi;

        /** */
        private DiscoverySpi discoSpi;

        /** */
        private CommunicationSpi commSpi;

        /** */
        private GridSpiTestContext spiCtx;

        /** */
        private Map<String, Object> allAttrs = new HashMap<>();

        /** */
        private IgniteTestResources rsrcs;

        /**
         *
         */
        TestData() throws IgniteCheckedException {
            rsrcs = new IgniteTestResources();
        }

        /**
         * @return Test resources.
         *
         */
        public IgniteTestResources getTestResources() {
            return rsrcs;
        }

        /**
         * @return {@code true} in case it is a discovery test.
         */
        @SuppressWarnings({"ObjectEquality"})
        public boolean isDiscoveryTest() {
            return spi == discoSpi;
        }

        /**
         * @return {@code true} in case it is a communication test.
         */
        @SuppressWarnings({"ObjectEquality"})
        public boolean isCommunicationTest() {
            return spi == commSpi;
        }

        /**
         * @return SPI.
         */
        public T getSpi() {
            return spi;
        }

        /**
         * @param spi SPI.
         */
        public void setSpi(T spi) {
            this.spi = spi;
        }

        /**
         * @param commSpi Communication SPI.
         */
        public void setCommSpi(CommunicationSpi commSpi) {
            this.commSpi = commSpi;
        }

        /**
         * @return Attributes.
         */
        public Map<String, Object> getAttributes() {
            return allAttrs;
        }

        /**
         * @param allAttrs Attributes.
         */
        public void setAllAttrs(Map<String, Object> allAttrs) {
            this.allAttrs = allAttrs;
        }

        /**
         * @return Discovery SPI.
         */
        public DiscoverySpi getDiscoverySpi() {
            return discoSpi;
        }

        /**
         * @param discoSpi Discovery SPI.
         */
        public void setDiscoverySpi(DiscoverySpi discoSpi) {
            this.discoSpi = discoSpi;
        }

        /**
         * @return SPI context.
         */
        public GridSpiTestContext getSpiContext() {
            return spiCtx;
        }

        /**
         * @param spiCtx SPI context.
         */
        public void setSpiContext(GridSpiTestContext spiCtx) {
            this.spiCtx = spiCtx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName() +
                " [spi=" + spi +
                ", discoSpi=" + discoSpi +
                ", allAttrs=" + allAttrs + ']';
        }
    }

    private static class SecurityPermissionSetImpl implements SecurityPermissionSet {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean defaultAllowAll() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<SecurityPermission>> taskPermissions() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<SecurityPermission>> cachePermissions() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Nullable @Override public Collection<SecurityPermission> systemPermissions() {
            return null;
        }
    }
}