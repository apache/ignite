/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.spi;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.security.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.communication.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.multicast.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.spi.GridSpiTestConfig.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.product.GridProductVersion.*;

/**
 * Base SPI test class.
 * @param <T> SPI implementation class.
 */
@SuppressWarnings({"JUnitTestCaseWithNonTrivialConstructors"})
public abstract class GridSpiAbstractTest<T extends GridSpi> extends GridAbstractTest {
    /** */
    private static final GridProductVersion VERSION = fromString("99.99.99");

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
    protected TestData<T> getTestData() {
        TestData<T> data = (TestData<T>)tests.get(getClass());

        if (data == null)
            tests.put(getClass(), data = new TestData<>());

        return data;
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
    @Override protected final GridTestResources getTestResources() {
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

        if (!(spi instanceof GridDiscoverySpi)) {
            if (spiTest.triggerDiscovery())
                configureDiscovery(spiTest);
        }
        else
            getTestData().setDiscoverySpi((GridDiscoverySpi)spi);

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

        GridDiscoverySpi discoSpi = getTestData().getDiscoverySpi();

        if (spiTest.triggerDiscovery() && !getTestData().isDiscoveryTest()) {
            getTestData().getAttributes().putAll(
                initSpiClassAndVersionAttributes(discoSpi));

            // Set all local node attributes into discovery SPI.
            discoSpi.setNodeAttributes(getTestData().getAttributes(), VERSION);

            discoSpi.setMetricsProvider(createMetricsProvider());

            discoSpi.setDataExchange(new GridDiscoverySpiDataExchange() {
                @Override public List<Object> collect(UUID nodeId) {
                    return new ArrayList<>();
                }

                @Override public void onExchange(List<Object> data) {
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

        if (spi instanceof GridDiscoverySpi) {
            getTestData().getAttributes().putAll(initSpiClassAndVersionAttributes(spi));

            ((GridDiscoverySpi)spi).setNodeAttributes(getTestData().getAttributes(), VERSION);

            ((GridDiscoverySpi)spi).setMetricsProvider(createMetricsProvider());
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
        if (discoSpi != null && !discoSpi.equals(spi)) {
            discoSpi.onContextInitialized(getSpiContext());
        }

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

            for (GridNode node : getTestData().getDiscoverySpi().getRemoteNodes())
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
    private Map<String, Serializable> initSpiClassAndVersionAttributes(GridSpi spi) {
        Map<String, Serializable> attrs = new HashMap<>();

        attrs.put(U.spiAttribute(spi, GridNodeAttributes.ATTR_SPI_CLASS), spi.getClass().getName());

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
        GridDiscoverySpi discoSpi = spiTest.discoverySpi().newInstance();

        if (discoSpi instanceof GridTcpDiscoverySpi) {
            GridTcpDiscoverySpi tcpDisco = (GridTcpDiscoverySpi)discoSpi;

            tcpDisco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));
        }

        getTestData().setDiscoverySpi(discoSpi);

        getTestResources().inject(discoSpi);

        discoSpi.setAuthenticator(new GridDiscoverySpiNodeAuthenticator() {
            @Override public GridSecurityContext authenticateNode(GridNode n, GridSecurityCredentials cred) {
                GridSecuritySubjectAdapter subj = new GridSecuritySubjectAdapter(
                    GridSecuritySubjectType.REMOTE_NODE, n.id());

                subj.permissions(new GridAllowAllPermissionSet());

                return new GridSecurityContext(subj);
            }
        });

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
    protected GridDiscoveryMetricsProvider createMetricsProvider() {
        return new GridDiscoveryMetricsProvider() {
            /** {@inheritDoc} */
            @Override public GridNodeMetrics getMetrics() { return new GridDiscoveryMetricsAdapter(); }
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
    private void configure(GridSpi spi) throws Exception {
        // Inject Configuration.
        for (Method m : getClass().getMethods()) {
            GridSpiTestConfig cfg = m.getAnnotation(GridSpiTestConfig.class);

            if (cfg != null) {
                if (getTestData().isDiscoveryTest() ||
                    (cfg.type() != ConfigType.DISCOVERY && !(spi instanceof GridDiscoverySpi)) ||
                    (cfg.type() != ConfigType.SELF && spi instanceof GridDiscoverySpi)) {
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
        if (spi instanceof GridTcpCommunicationSpi)
            ((GridTcpCommunicationSpi)spi).setLocalPort(GridTestUtils.getNextCommPort(getClass()));

        if (spi instanceof GridTcpDiscoverySpi) {
            GridTcpDiscoveryIpFinder ipFinder = ((GridTcpDiscoverySpi)spi).getIpFinder();

            if (ipFinder instanceof GridTcpDiscoveryMulticastIpFinder) {
                String mcastAddr = GridTestUtils.getNextMulticastGroup(getClass());

                if (mcastAddr != null && !mcastAddr.isEmpty()) {
                    ((GridTcpDiscoveryMulticastIpFinder)ipFinder).setMulticastGroup(mcastAddr);
                    ((GridTcpDiscoveryMulticastIpFinder)ipFinder).setMulticastPort(
                        GridTestUtils.getNextMulticastPort(getClass()));
                }
            }
        }
    }

    /**
     * @param spi SPI.
     * @throws Exception If failed.
     */
    protected void spiStart(GridSpi spi) throws Exception {
        U.setWorkDirectory(null, U.getGridGainHome());

        // Start SPI with unique grid name.
        spi.spiStart(getTestGridName());

        info("SPI started [spi=" + spi.getClass() + ']');
    }

    /**
     * @return  Fully initialized and started SPI implementation.
     */
    protected final T getSpi() {
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
    @Nullable protected final GridDiscoverySpi getDiscoverySpi() throws Exception {
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
    protected void spiStop(GridSpi spi) throws Exception {
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
        private GridDiscoverySpi discoSpi;

        /** */
        private GridCommunicationSpi commSpi;

        /** */
        private GridSpiTestContext spiCtx;

        /** */
        private Map<String, Object> allAttrs = new HashMap<>();

        /** */
        private GridTestResources rsrcs = new GridTestResources();

        /**
         *
         */
        TestData() {
            // No-op.
        }

        /**
         * @return Test resources.
         *
         */
        public GridTestResources getTestResources() {
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
        public void setCommSpi(GridCommunicationSpi commSpi) {
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
        public GridDiscoverySpi getDiscoverySpi() {
            return discoSpi;
        }

        /**
         * @param discoSpi Discovery SPI.
         */
        public void setDiscoverySpi(GridDiscoverySpi discoSpi) {
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
}
