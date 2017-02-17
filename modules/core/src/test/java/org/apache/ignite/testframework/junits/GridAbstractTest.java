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

package org.apache.ignite.testframework.junits;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.binary.BinaryEnumCache;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.GridClassLoaderCache;
import org.apache.ignite.internal.util.GridTestClockTimer;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.ignite.testframework.junits.multijvm.IgniteCacheProcessProxy;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.thread.IgniteThread;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.apache.log4j.RollingFileAppender;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.config.GridTestProperties.BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER;

/**
 * Common abstract test for Ignite tests.
 */
@SuppressWarnings({
    "TransientFieldInNonSerializableClass",
    "ProhibitedExceptionDeclared",
    "JUnitTestCaseWithNonTrivialConstructors"
})
public abstract class GridAbstractTest extends TestCase {
    /**************************************************************
     * DO NOT REMOVE TRANSIENT - THIS OBJECT MIGHT BE TRANSFERRED *
     *                  TO ANOTHER NODE.                          *
     **************************************************************/
    /** Null name for execution map. */
    private static final String NULL_NAME = UUID.randomUUID().toString();

    /** */
    private static final boolean BINARY_MARSHALLER = false;

    /** Ip finder for TCP discovery. */
    public static final TcpDiscoveryIpFinder LOCAL_IP_FINDER = new TcpDiscoveryVmIpFinder(false) {{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    /** */
    private static final long DFLT_TEST_TIMEOUT = 5 * 60 * 1000;

    /** */
    private static final transient Map<Class<?>, TestCounters> tests = new ConcurrentHashMap<>();

    /** */
    private transient boolean startGrid;

    /** */
    protected transient IgniteLogger log;

    /** */
    private transient ClassLoader clsLdr;

    /** */
    private transient boolean stopGridErr;

    /** Timestamp for tests. */
    private static long ts = System.currentTimeMillis();

    /** Starting grid name. */
    protected static final ThreadLocal<String> startingGrid = new ThreadLocal<>();

    /** Force failure flag. */
    private boolean forceFailure;

    /** Force failure message. */
    private String forceFailureMsg;

    /** Whether test count is known is advance. */
    private boolean forceTestCnt;

    /** Number of tests. */
    private int testCnt;

    /**
     *
     */
    static {
        System.setProperty(IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "10000");
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");
        System.setProperty(IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY, "1");

        if (BINARY_MARSHALLER)
            GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, BinaryMarshaller.class.getName());

        Thread timer = new Thread(new GridTestClockTimer(), "ignite-clock-for-tests");

        timer.setDaemon(true);

        timer.setPriority(10);

        timer.start();
    }

    /** */
    private static final ConcurrentMap<UUID, Object> serializedObj = new ConcurrentHashMap<>();

    /** */
    protected GridAbstractTest() throws IgniteCheckedException {
        this(false);

        log = getTestCounters().getTestResources().getLogger().getLogger(getClass());
    }

    /**
     * @param startGrid Start grid flag.
     */
    @SuppressWarnings({"OverriddenMethodCallDuringObjectConstruction"})
    protected GridAbstractTest(boolean startGrid) {
        assert isJunitFrameworkClass() : "GridAbstractTest class cannot be extended directly " +
            "(use GridCommonAbstractTest class instead).";

        // Initialize properties. Logger initialized here.
        GridTestProperties.init();

        log = new GridTestLog4jLogger();

        this.startGrid = startGrid;
    }

    /**
     * @param cls Class to create.
     * @return Instance of class.
     * @throws Exception If failed.
     */
    protected <T> T allocateInstance(Class<T> cls) throws Exception {
        return (T)GridUnsafe.allocateInstance(cls);
    }

    /**
     * @param cls Class to create.
     * @return Instance of class.
     */
    @Nullable protected <T> T allocateInstance0(Class<T> cls) {
        try {
            return (T)GridUnsafe.allocateInstance(cls);
        }
        catch (InstantiationException e) {
            e.printStackTrace();

            return null;
        }
    }

    /**
     * @return Flag to check if class is Junit framework class.
     */
    protected boolean isJunitFrameworkClass() {
        return false;
    }

    /**
     * @return Test resources.
     */
    protected IgniteTestResources getTestResources() throws IgniteCheckedException {
        return getTestCounters().getTestResources();
    }

    /**
     * @param msg Message to print.
     */
    protected void info(String msg) {
        if (log().isInfoEnabled())
            log().info(msg);
    }

    /**
     * @param msg Message to print.
     */
    protected void error(String msg) {
        log().error(msg);
    }

    /**
     * @param msg Message to print.
     * @param t Error to print.
     */
    protected void error(String msg, Throwable t) {
        log().error(msg, t);
    }

    /**
     * @return logger.
     */
    protected IgniteLogger log() {
        if (isRemoteJvm())
            return IgniteNodeRunner.startedInstance().log();

        return log;
    }

    /**
     * Resets log4j programmatically.
     *
     * @param log4jLevel Level.
     * @param logToFile If {@code true}, then log to file under "work/log" folder.
     * @param cat Category.
     * @param cats Additional categories.
     */
    @SuppressWarnings({"deprecation"})
    protected void resetLog4j(Level log4jLevel, boolean logToFile, String cat, String... cats)
        throws IgniteCheckedException {
        for (String c : F.concat(false, cat, F.asList(cats)))
            Logger.getLogger(c).setLevel(log4jLevel);

        if (logToFile) {
            Logger log4j = Logger.getRootLogger();

            log4j.removeAllAppenders();

            // Console appender.
            ConsoleAppender c = new ConsoleAppender();

            c.setName("CONSOLE_ERR");
            c.setTarget("System.err");
            c.setThreshold(Priority.WARN);
            c.setLayout(new PatternLayout("[%d{ABSOLUTE}][%-5p][%t][%c{1}] %m%n"));

            c.activateOptions();

            log4j.addAppender(c);

            // File appender.
            RollingFileAppender file = new RollingFileAppender();

            file.setName("FILE");
            file.setThreshold(log4jLevel);
            file.setFile(home() + "/work/log/ignite.log");
            file.setAppend(false);
            file.setMaxFileSize("10MB");
            file.setMaxBackupIndex(10);
            file.setLayout(new PatternLayout("[%d{ABSOLUTE}][%-5p][%t][%c{1}] %m%n"));

            file.activateOptions();

            log4j.addAppender(file);
        }
    }

    /**
     * Executes runnable and prints out how long it took.
     *
     * @param name Name of execution.
     * @param r Runnable to execute.
     */
    protected void time(String name, Runnable r) {
        System.gc();

        long start = System.currentTimeMillis();

        r.run();

        long dur = System.currentTimeMillis() - start;

        info(name + " took " + dur + "ms.");
    }

    /**
     * Runs given code in multiple threads and synchronously waits for all threads to complete.
     * If any thread failed, exception will be thrown out of this method.
     *
     * @param r Runnable.
     * @param threadNum Thread number.
     * @throws Exception If failed.
     */
    protected void multithreaded(Runnable r, int threadNum) throws Exception {
        multithreaded(r, threadNum, getTestGridName());
    }

    /**
     * Runs given code in multiple threads and synchronously waits for all
     * threads to complete. If any thread failed, exception will be thrown
     * out of this method.
     *
     * @param r Runnable.
     * @param threadNum Thread number.
     * @param threadName Thread name.
     * @throws Exception If failed.
     */
    protected void multithreaded(Runnable r, int threadNum, String threadName) throws Exception {
        GridTestUtils.runMultiThreaded(r, threadNum, threadName);
    }

    /**
     * Runs given code in multiple threads. Returns future that ends upon
     * threads completion. If any thread failed, exception will be thrown
     * out of this method.
     *
     * @param r Runnable.
     * @param threadNum Thread number.
     * @throws Exception If failed.
     * @return Future.
     */
    protected IgniteInternalFuture<?> multithreadedAsync(Runnable r, int threadNum) throws Exception {
        return multithreadedAsync(r, threadNum, getTestGridName());
    }

    /**
     * Runs given code in multiple threads. Returns future that ends upon
     * threads completion. If any thread failed, exception will be thrown
     * out of this method.
     *
     * @param r Runnable.
     * @param threadNum Thread number.
     * @param threadName Thread name.
     * @throws Exception If failed.
     * @return Future.
     */
    protected IgniteInternalFuture<?> multithreadedAsync(Runnable r, int threadNum, String threadName) throws Exception {
        return GridTestUtils.runMultiThreadedAsync(r, threadNum, threadName);
    }

    /**
     * Runs given code in multiple threads and synchronously waits for all threads to complete.
     * If any thread failed, exception will be thrown out of this method.
     *
     * @param c Callable.
     * @param threadNum Thread number.
     * @throws Exception If failed.
     */
    protected void multithreaded(Callable<?> c, int threadNum) throws Exception {
        multithreaded(c, threadNum, getTestGridName());
    }

    /**
     * Runs given code in multiple threads and synchronously waits for all threads to complete.
     * If any thread failed, exception will be thrown out of this method.
     *
     * @param c Callable.
     * @param threadNum Thread number.
     * @param threadName Thread name.
     * @throws Exception If failed.
     */
    protected void multithreaded(Callable<?> c, int threadNum, String threadName) throws Exception {
        GridTestUtils.runMultiThreaded(c, threadNum, threadName);
    }

    /**
     * Runs given code in multiple threads and asynchronously waits for all threads to complete.
     * If any thread failed, exception will be thrown out of this method.
     *
     * @param c Callable.
     * @param threadNum Thread number.
     * @throws Exception If failed.
     * @return Future.
     */
    protected IgniteInternalFuture<?> multithreadedAsync(Callable<?> c, int threadNum) throws Exception {
        return multithreadedAsync(c, threadNum, getTestGridName());
    }

    /**
     * Runs given code in multiple threads and asynchronously waits for all threads to complete.
     * If any thread failed, exception will be thrown out of this method.
     *
     * @param c Callable.
     * @param threadNum Thread number.
     * @param threadName Thread name.
     * @throws Exception If failed.
     * @return Future.
     */
    protected IgniteInternalFuture<?> multithreadedAsync(Callable<?> c, int threadNum, String threadName) throws Exception {
        return GridTestUtils.runMultiThreadedAsync(c, threadNum, threadName);
    }

    /**
     * @return Test kernal context.
     */
    protected GridTestKernalContext newContext() throws IgniteCheckedException {
        return new GridTestKernalContext(log());
    }

    /**
     * @param cfg Configuration to use in Test
     * @return Test kernal context.
     */
    protected GridTestKernalContext newContext(IgniteConfiguration cfg) throws IgniteCheckedException {
        return new GridTestKernalContext(log(), cfg);
    }

    /**
     * Called before execution of every test method in class.
     *
     * @throws Exception If failed. {@link #afterTest()} will be called in this case.
     */
    protected void beforeTest() throws Exception {
        // No-op.
    }

    /**
     * Called after execution of every test method in class or
     * if {@link #beforeTest()} failed without test method execution.
     *
     * @throws Exception If failed.
     */
    protected void afterTest() throws Exception {
        // No-op.
    }

    /**
     * Called before execution of all test methods in class.
     *
     * @throws Exception If failed. {@link #afterTestsStopped()} will be called in this case.
     */
    protected void beforeTestsStarted() throws Exception {
        // Will clean and re-create marshaller directory from scratch.
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", true);
    }

    /**
     * Called after execution of all test methods in class or
     * if {@link #beforeTestsStarted()} failed without execution of any test methods.
     *
     * @throws Exception If failed.
     */
    protected void afterTestsStopped() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        stopGridErr = false;

        clsLdr = Thread.currentThread().getContextClassLoader();

        // Change it to the class one.
        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

        // Clear log throttle.
        LT.clear();

        TestCounters cntrs = getTestCounters();

        if (isDebug())
            info("Test counters [numOfTests=" + cntrs.getNumberOfTests() + ", started=" + cntrs.getStarted() +
                ", stopped=" + cntrs.getStopped() + ']');

        if (cntrs.isReset()) {
            info("Resetting test counters.");

            int started = cntrs.getStarted() % cntrs.getNumberOfTests();
            int stopped = cntrs.getStopped() % cntrs.getNumberOfTests();

            cntrs.reset();

            cntrs.setStarted(started);
            cntrs.setStopped(stopped);
        }

        if (isFirstTest()) {
            info(">>> Starting test class: " + testClassDescription() + " <<<");

            if (startGrid) {
                IgniteConfiguration cfg = optimize(getConfiguration());

                G.start(cfg);
            }

            try {
                List<Integer> jvmIds = IgniteNodeRunner.killAll();

                if (!jvmIds.isEmpty())
                    log.info("Next processes of IgniteNodeRunner were killed: " + jvmIds);

                beforeTestsStarted();
            }
            catch (Exception | Error t) {
                t.printStackTrace();

                getTestCounters().setStopped(getTestCounters().getNumberOfTests() - 1);

                try {
                    tearDown();
                }
                catch (Exception e) {
                    log.error("Failed to tear down test after exception was thrown in beforeTestsStarted (will " +
                        "ignore)", e);
                }

                throw t;
            }
        }

        info(">>> Starting test: " + testDescription() + " <<<");

        try {
            beforeTest();
        }
        catch (Exception | Error t) {
            try {
                tearDown();
            }
            catch (Exception e) {
                log.error("Failed to tear down test after exception was thrown in beforeTest (will ignore)", e);
            }

            throw t;
        }

        ts = System.currentTimeMillis();
    }

    /**
     * @return Test description.
     */
    protected String testDescription() {
        return GridTestUtils.fullSimpleName(getClass()) + "#" + getName();
    }

    /**
     * @return Test class description.
     */
    protected String testClassDescription() {
        return GridTestUtils.fullSimpleName(getClass());
    }

    /**
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected Ignite startGrid() throws Exception {
        return startGrid(getTestGridName());
    }

    /**
     * @param cnt Grid count.
     * @return First started grid.
     * @throws Exception If failed.
     */
    protected final Ignite startGrids(int cnt) throws Exception {
        assert cnt > 0;

        Ignite ignite = null;

        for (int i = 0; i < cnt; i++)
            if (ignite == null)
                ignite = startGrid(i);
            else
                startGrid(i);

        checkTopology(cnt);

        assert ignite != null;

        return ignite;
    }

    /**
     * @param cnt Grid count.
     * @return First started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGridsMultiThreaded(int cnt) throws Exception {
        if (cnt == 1)
            return startGrids(1);

        Ignite ignite = startGridsMultiThreaded(0, cnt);

        checkTopology(cnt);

        return ignite;
    }

    /**
     * @param init Start grid index.
     * @param cnt Grid count.
     * @return First started grid.
     * @throws Exception If failed.
     */
    protected final Ignite startGridsMultiThreaded(int init, int cnt) throws Exception {
        if (isMultiJvm())
            fail("https://issues.apache.org/jira/browse/IGNITE-648");

        assert init >= 0;
        assert cnt > 0;

        info("Starting grids: " + cnt);

        final AtomicInteger gridIdx = new AtomicInteger(init);

        GridTestUtils.runMultiThreaded(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    startGrid(gridIdx.getAndIncrement());

                    return null;
                }
            },
            cnt,
            "grid-starter-" + getName()
        );

        assert gridIdx.get() - init == cnt;

        return grid(init);
    }

    /**
     * @param cnt Grid count
     * @throws Exception If an error occurs.
     */
    @SuppressWarnings({"BusyWait"})
    protected void checkTopology(int cnt) throws Exception {
        for (int j = 0; j < 10; j++) {
            boolean topOk = true;

            for (int i = 0; i < cnt; i++) {
                if (cnt != grid(i).cluster().nodes().size()) {
                    U.warn(log, "Grid size is incorrect (will re-run check in 1000 ms) " +
                        "[name=" + grid(i).name() + ", size=" + grid(i).cluster().nodes().size() + ']');

                    topOk = false;

                    break;
                }
            }

            if (topOk)
                return;
            else
                Thread.sleep(1000);
        }

        throw new Exception("Failed to wait for proper topology: " + cnt);
    }

    /** */
    protected void stopGrid() {
        stopGrid(getTestGridName());
    }

    /**
     * Starts new grid with given index.
     *
     * @param idx Index of the grid to start.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected IgniteEx startGrid(int idx) throws Exception {
        return (IgniteEx)startGrid(getTestGridName(idx));
    }

    /**
     * Starts new grid with given index and Spring application context.
     *
     * @param idx Index of the grid to start.
     * @param ctx Spring context.
     * @return Started grid.
     * @throws Exception If anything failed.
     */
    protected Ignite startGrid(int idx, GridSpringResourceContext ctx) throws Exception {
        return startGrid(getTestGridName(idx), ctx);
    }

    /**
     * Starts new grid with given name.
     *
     * @param gridName Grid name.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String gridName) throws Exception {
        return startGrid(gridName, (GridSpringResourceContext)null);
    }

    /**
     * Starts new grid with given name.
     *
     * @param gridName Grid name.
     * @param ctx Spring context.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String gridName, GridSpringResourceContext ctx) throws Exception {
        return startGrid(gridName, optimize(getConfiguration(gridName)), ctx);
    }
    /**
     * Starts new grid with given name.
     *
     * @param gridName Grid name.
     * @param ctx Spring context.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String gridName, IgniteConfiguration cfg, GridSpringResourceContext ctx)
        throws Exception {
        if (!isRemoteJvm(gridName)) {
            startingGrid.set(gridName);

            try {
                Ignite node = IgnitionEx.start(cfg, ctx);

                IgniteConfiguration nodeCfg = node.configuration();

                log.info("Node started with the following configuration [id=" + node.cluster().localNode().id()
                    + ", marshaller=" + nodeCfg.getMarshaller()
                    + ", binaryCfg=" + nodeCfg.getBinaryConfiguration()
                    + ", lateAff=" + nodeCfg.isLateAffinityAssignment() + "]");

                return node;
            }
            finally {
                startingGrid.set(null);
            }
        }
        else
            return startRemoteGrid(gridName, null, ctx);
    }

    /**
     * Starts new grid at another JVM with given name.
     *
     * @param gridName Grid name.
     * @param ctx Spring context.
     * @return Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startRemoteGrid(String gridName, IgniteConfiguration cfg, GridSpringResourceContext ctx)
        throws Exception {
        if (ctx != null)
            throw new UnsupportedOperationException("Starting of grid at another jvm by context doesn't supported.");

        if (cfg == null)
            cfg = optimize(getConfiguration(gridName));

        return new IgniteProcessProxy(cfg, log, grid(0));
    }

    /**
     * Optimizes configuration to achieve better test performance.
     *
     * @param cfg Configuration.
     * @return Optimized configuration (by modifying passed in one).
     */
    protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        // TODO: IGNITE-605: propose another way to avoid network overhead in tests.
        if (cfg.getLocalHost() == null) {
            if (cfg.getDiscoverySpi() instanceof TcpDiscoverySpi) {
                cfg.setLocalHost("127.0.0.1");

                if (((TcpDiscoverySpi)cfg.getDiscoverySpi()).getJoinTimeout() == 0)
                    ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setJoinTimeout(8000);
            }
            else
                cfg.setLocalHost(getTestResources().getLocalHost());
        }

        // Do not add redundant data if it is not needed.
        if (cfg.getIncludeProperties() == null)
            cfg.setIncludeProperties();

        return cfg;
    }

    /**
     * @param gridName Grid name.
     */
    protected void stopGrid(@Nullable String gridName) {
        stopGrid(gridName, true);
    }

    /**
     * @param gridName Grid name.
     * @param cancel Cancel flag.
     */
    @SuppressWarnings({"deprecation"})
    protected void stopGrid(@Nullable String gridName, boolean cancel) {
        try {
            Ignite ignite = grid(gridName);

            assert ignite != null : "Ignite returned null grid for name: " + gridName;

            info(">>> Stopping grid [name=" + ignite.name() + ", id=" +
                ((IgniteKernal)ignite).context().localNodeId() + ']');

            if (!isRemoteJvm(gridName))
                G.stop(gridName, cancel);
            else
                IgniteProcessProxy.stop(gridName, cancel);
        }
        catch (IllegalStateException ignored) {
            // Ignore error if grid already stopped.
        }
        catch (Throwable e) {
            error("Failed to stop grid [gridName=" + gridName + ", cancel=" + cancel + ']', e);

            stopGridErr = true;
        }
    }

    /**
     *
     */
    protected void stopAllGrids() {
        stopAllGrids(true);
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void stopAllGrids(boolean cancel) {
        try {
            Collection<Ignite> clients = new ArrayList<>();
            Collection<Ignite> srvs = new ArrayList<>();

            for (Ignite g : G.allGrids()) {
                if (g.configuration().getDiscoverySpi().isClientMode())
                    clients.add(g);
                else
                    srvs.add(g);
            }

            for (Ignite g : clients)
                stopGrid(g.name(), cancel);

            for (Ignite g : srvs)
                stopGrid(g.name(), cancel);

            assert G.allGrids().isEmpty();
        }
        finally {
            IgniteProcessProxy.killAll(); // In multi-JVM case.
        }
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void stopAllClients(boolean cancel) {
        List<Ignite> ignites = G.allGrids();

        for (Ignite g : ignites) {
            if (g.configuration().getDiscoverySpi().isClientMode())
                stopGrid(g.name(), cancel);
        }
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void stopAllServers(boolean cancel) {
        List<Ignite> ignites = G.allGrids();

        for (Ignite g : ignites) {
            if (!g.configuration().getDiscoverySpi().isClientMode())
                stopGrid(g.name(), cancel);
        }
    }

    /**
     * @param ignite Grid
     * @param cnt Count
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"BusyWait"})
    protected void waitForRemoteNodes(Ignite ignite, int cnt) throws IgniteCheckedException {
        while (true) {
            Collection<ClusterNode> nodes = ignite.cluster().forRemotes().nodes();

            if (nodes != null && nodes.size() >= cnt)
                return;

            try {
                Thread.sleep(100);
            }
            catch (InterruptedException ignored) {
                throw new IgniteCheckedException("Interrupted while waiting for remote nodes [gridName=" + ignite.name() +
                    ", count=" + cnt + ']');
            }
        }
    }

    /**
     * @param ignites Grids
     * @throws IgniteCheckedException If failed.
     */
    protected void waitForDiscovery(Ignite... ignites) throws IgniteCheckedException {
        assert ignites != null;
        assert ignites.length > 1;

        for (Ignite ignite : ignites)
            waitForRemoteNodes(ignite, ignites.length - 1);
    }

    /**
     * Gets grid for given name.
     *
     * @param name Name.
     * @return Grid instance.
     */
    protected IgniteEx grid(String name) {
        if (!isRemoteJvm(name))
            return (IgniteEx)G.ignite(name);
        else {
            if (isRemoteJvm())
                return IgniteNodeRunner.startedInstance();
            else
                return IgniteProcessProxy.ignite(name);
        }
    }

    /**
     * Gets grid for given index.
     *
     * @param idx Index.
     * @return Grid instance.
     */
    protected IgniteEx grid(int idx) {
        return grid(getTestGridName(idx));
    }

    /**
     * @param idx Index.
     * @return Ignite instance.
     */
    protected Ignite ignite(int idx) {
        return grid(idx);
    }

    /**
     * Gets grid for given test.
     *
     * @return Grid for given test.
     */
    protected IgniteEx grid() {
        if (!isMultiJvm())
            return (IgniteEx)G.ignite(getTestGridName());
        else
            throw new UnsupportedOperationException("Operation doesn't supported yet.");
    }

    /**
     * @param node Node.
     * @return Ignite instance with given local node.
     */
    protected final Ignite grid(ClusterNode node) {
        if (!isMultiJvm())
            return G.ignite(node.id());
        else {
            try {
                return IgniteProcessProxy.ignite(node.id());
            }
            catch (Exception ignore) {
                // A hack if it is local grid.
                return G.ignite(node.id());
            }
        }
    }

    /**
     * Starts grid using provided grid name and spring config location.
     * <p>
     * Note that grids started this way should be stopped with {@code G.stop(..)} methods.
     *
     * @param gridName Grid name.
     * @param springCfgPath Path to config file.
     * @return Grid Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String gridName, String springCfgPath) throws Exception {
        return startGrid(gridName, loadConfiguration(springCfgPath));
    }

    /**
     * Starts grid using provided grid name and config.
     * <p>
     * Note that grids started this way should be stopped with {@code G.stop(..)} methods.
     *
     * @param gridName Grid name.
     * @param cfg Config.
     * @return Grid Started grid.
     * @throws Exception If failed.
     */
    protected Ignite startGrid(String gridName, IgniteConfiguration cfg) throws Exception {
        cfg.setGridName(gridName);

        if (!isRemoteJvm(gridName))
            return G.start(cfg);
        else
            return startRemoteGrid(gridName, cfg, null);
    }

    /**
     * Loads configuration from the given Spring XML file.
     *
     * @param springCfgPath Path to file.
     * @return Grid configuration.
     * @throws IgniteCheckedException If load failed.
     */
    @SuppressWarnings("deprecation")
    protected IgniteConfiguration loadConfiguration(String springCfgPath) throws IgniteCheckedException {
        URL cfgLocation = U.resolveIgniteUrl(springCfgPath);

        if (cfgLocation == null)
            cfgLocation = U.resolveIgniteUrl(springCfgPath, false);

        assert cfgLocation != null;

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(cfgLocation.toString());
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate Spring XML application context.", e);
        }

        Map cfgMap;

        try {
            // Note: Spring is not generics-friendly.
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null)
            throw new IgniteCheckedException("Failed to find a single grid factory configuration in: " + springCfgPath);

        if (cfgMap.isEmpty())
            throw new IgniteCheckedException("Can't find grid factory configuration in: " + springCfgPath);
        else if (cfgMap.size() > 1)
            throw new IgniteCheckedException("More than one configuration provided for cache load test: " + cfgMap.values());

        IgniteConfiguration cfg = (IgniteConfiguration)cfgMap.values().iterator().next();

        cfg.setNodeId(UUID.randomUUID());

        return cfg;
    }

    /**
     * @param idx Index of the grid to stop.
     */
    protected void stopGrid(int idx) {
        stopGrid(getTestGridName(idx), false);
    }

    /**
     * @param idx Grid index.
     * @param cancel Cancel flag.
     */
    @SuppressWarnings("deprecation")
    protected void stopGrid(int idx, boolean cancel) {
        String gridName = getTestGridName(idx);

        try {
            Ignite ignite = G.ignite(gridName);

            assert ignite != null : "Ignite returned null grid for name: " + gridName;

            info(">>> Stopping grid [name=" + ignite.name() + ", id=" + ignite.cluster().localNode().id() + ']');

            G.stop(gridName, cancel);
        }
        catch (IllegalStateException ignored) {
            // Ignore error if grid already stopped.
        }
        catch (Throwable e) {
            error("Failed to stop grid [gridName=" + gridName + ", cancel=" + cancel + ']', e);

            stopGridErr = true;
        }
    }

    /**
     * @param idx Index of the grid to stop.
     */
    protected void stopAndCancelGrid(int idx) {
        stopGrid(getTestGridName(idx), true);
    }

    /**
     * @return Grid test configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration getConfiguration() throws Exception {
        // Generate unique grid name.
        return getConfiguration(getTestGridName());
    }

    /**
     * This method should be overridden by subclasses to change configuration parameters.
     *
     * @param gridName Grid name.
     * @return Grid configuration used for starting of grid.
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(gridName, getTestResources());

        cfg.setNodeId(null);

        if (GridTestProperties.getProperty(GridTestProperties.BINARY_COMPACT_FOOTERS) != null) {
            if (!Boolean.valueOf(GridTestProperties.getProperty(GridTestProperties.BINARY_COMPACT_FOOTERS))) {
                BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

                if (bCfg == null) {
                    bCfg = new BinaryConfiguration();

                    cfg.setBinaryConfiguration(bCfg);
                }

                bCfg.setCompactFooter(false);
            }
        }

        if (Boolean.valueOf(GridTestProperties.getProperty(BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER))) {
            BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

            if (bCfg == null) {
                bCfg = new BinaryConfiguration();

                cfg.setBinaryConfiguration(bCfg);
            }

            bCfg.setNameMapper(new BinaryBasicNameMapper(true));
        }

        if (gridName != null && gridName.matches(".*\\d")) {
            String idStr = UUID.randomUUID().toString();

            if (gridName.startsWith(getTestGridName())) {
                String idxStr = String.valueOf(getTestGridIndex(gridName));

                while (idxStr.length() < 5)
                    idxStr = '0' + idxStr;

                char[] chars = idStr.toCharArray();

                for (int i = 0; i < idxStr.length(); i++)
                    chars[chars.length - idxStr.length() + i] = idxStr.charAt(i);

                cfg.setNodeId(UUID.fromString(new String(chars)));
            }
            else {
                char[] chars = idStr.toCharArray();

                chars[0] = gridName.charAt(gridName.length() - 1);
                chars[1] = '0';

                chars[chars.length - 3] = '0';
                chars[chars.length - 2] = '0';
                chars[chars.length - 1] = gridName.charAt(gridName.length() - 1);

                cfg.setNodeId(UUID.fromString(new String(chars)));
            }
        }

        if (isMultiJvm())
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(LOCAL_IP_FINDER);

        return cfg;
    }

    /**
     * @return Generated unique test grid name.
     */
    public String getTestGridName() {
        String[] parts = getClass().getName().split("\\.");

        return parts[parts.length - 2] + '.' + parts[parts.length - 1];
    }

    /**
     * @param idx Index of the grid.
     * @return Indexed grid name.
     */
    public String getTestGridName(int idx) {
        return getTestGridName() + idx;
    }

    /**
     * Parses test grid index from test grid name.
     *
     * @param testGridName Test grid name, returned by {@link #getTestGridName(int)}.
     * @return Test grid index.
     */
    public int getTestGridIndex(String testGridName) {
        return Integer.parseInt(testGridName.substring(getTestGridName().length()));
    }

    /**
     * @return {@code True} if system property -DDEBUG is set.
     */
    public boolean isDebug() {
        return System.getProperty("DEBUG") != null;
    }

    /**
     * @param marshaller Marshaller to get checkpoint path for.
     * @return Path for specific marshaller.
     */
    @SuppressWarnings({"IfMayBeConditional", "deprecation"})
    protected String getDefaultCheckpointPath(Marshaller marshaller) {
        if (marshaller instanceof JdkMarshaller)
            return SharedFsCheckpointSpi.DFLT_DIR_PATH + "/jdk/";
        else
            return SharedFsCheckpointSpi.DFLT_DIR_PATH + '/' + marshaller.getClass().getSimpleName() + '/';
    }

    /**
     * @param name Name to mask.
     * @return Masked name.
     */
    private String maskNull(String name) {
        return name == null ? NULL_NAME : name;
    }

    /**
     * @return Ignite home.
     */
    protected String home() throws IgniteCheckedException {
        return getTestResources().getIgniteHome();
    }

    /**
     * This method should be overridden by subclasses to change configuration parameters.
     *
     * @return Grid configuration used for starting of grid.
     * @param gridName Grid name.
     * @param rsrcs Resources.
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    protected IgniteConfiguration getConfiguration(String gridName, IgniteTestResources rsrcs) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);
        cfg.setGridLogger(rsrcs.getLogger());
        cfg.setMarshaller(rsrcs.getMarshaller());
        cfg.setNodeId(rsrcs.getNodeId());
        cfg.setIgniteHome(rsrcs.getIgniteHome());
        cfg.setMBeanServer(rsrcs.getMBeanServer());
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setMetricsLogFrequency(0);

        cfg.setConnectorConfiguration(null);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        commSpi.setTcpNoDelay(true);

        cfg.setCommunicationSpi(commSpi);

        TcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();

        if (isDebug()) {
            discoSpi.setMaxMissedHeartbeats(Integer.MAX_VALUE);
            cfg.setNetworkTimeout(Long.MAX_VALUE / 3);
        }
        else {
            // Set network timeout to 10 sec to avoid unexpected p2p class loading errors.
            cfg.setNetworkTimeout(10000);

            // Increase max missed heartbeats to avoid unexpected node fails.
            discoSpi.setMaxMissedHeartbeats(30);
        }

        // Set heartbeat interval to 1 second to speed up tests.
        discoSpi.setHeartbeatFrequency(1000);

        String mcastAddr = GridTestUtils.getNextMulticastGroup(getClass());

        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setAddresses(Collections.singleton("127.0.0.1:" + TcpDiscoverySpi.DFLT_PORT));

        if (!F.isEmpty(mcastAddr)) {
            ipFinder.setMulticastGroup(mcastAddr);
            ipFinder.setMulticastPort(GridTestUtils.getNextMulticastPort(getClass()));
        }

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        SharedFsCheckpointSpi cpSpi = new SharedFsCheckpointSpi();

        Collection<String> paths = new ArrayList<>();

        paths.add(getDefaultCheckpointPath(cfg.getMarshaller()));

        cpSpi.setDirectoryPaths(paths);

        cfg.setCheckpointSpi(cpSpi);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * @return New cache configuration with modified defaults.
     */
    public static CacheConfiguration defaultCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setStartSize(1024);
        cfg.setAtomicWriteOrderMode(PRIMARY);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setEvictionPolicy(null);

        return cfg;
    }

    /**
     * Gets external class loader.
     *
     * @return External class loader.
     */
    protected static ClassLoader getExternalClassLoader() {
        String path = GridTestProperties.getProperty("p2p.uri.cls");

        try {
            return new URLClassLoader(new URL[] {new URL(path)}, U.gridClassLoader());
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create URL: " + path, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void tearDown() throws Exception {
        long dur = System.currentTimeMillis() - ts;

        info(">>> Stopping test: " + testDescription() + " in " + dur + " ms <<<");

        TestCounters cntrs = getTestCounters();

        if (isDebug())
            info("Test counters [numOfTests=" + cntrs.getNumberOfTests() + ", started=" + cntrs.getStarted() +
                ", stopped=" + cntrs.getStopped() + ']');

        try {
            afterTest();
        }
        finally {
            serializedObj.clear();

            if (isLastTest()) {
                info(">>> Stopping test class: " + testClassDescription() + " <<<");

                TestCounters counters = getTestCounters();

                // Stop all threads started by runMultithreaded() methods.
                GridTestUtils.stopThreads(log);

                // Safety.
                getTestResources().stopThreads();

                // Set reset flags, so counters will be reset on the next setUp.
                counters.setReset(true);

                afterTestsStopped();

                if (startGrid)
                    G.stop(getTestGridName(), true);

                // Remove counters.
                tests.remove(getClass());

                // Remove resources cached in static, if any.
                GridClassLoaderCache.clear();
                U.clearClassCache();
                MarshallerExclusions.clearCache();
                BinaryEnumCache.clear();
            }

            Thread.currentThread().setContextClassLoader(clsLdr);

            clsLdr = null;

            cleanReferences();
        }
    }

    /**
     *
     */
    protected void cleanReferences() {
        Class cls = getClass();

        while (cls != null) {
            Field[] fields = getClass().getDeclaredFields();

            for (Field f : fields) {
                if (Modifier.isStatic(f.getModifiers()))
                    continue;

                f.setAccessible(true);

                try {
                    f.set(this, null);
                }
                catch (Exception ignored) {
                }
            }

            cls = cls.getSuperclass();
        }
    }

    /**
     * @return First test flag.
     */
    protected boolean isFirstTest() throws IgniteCheckedException {
        TestCounters cntrs = getTestCounters();

        return cntrs.getStarted() == 1 && cntrs.getStopped() == 0;
    }

    /**
     * @return Last test flag.
     */
    protected boolean isLastTest() throws IgniteCheckedException {
        TestCounters cntrs = getTestCounters();

        return cntrs.getStopped() == cntrs.getNumberOfTests();
    }

    /**
     * Gets flag whether nodes will run in one JVM or in separate JVMs.
     *
     * @return <code>True</code> to run nodes in separate JVMs.
     * @see IgniteNodeRunner
     * @see IgniteProcessProxy
     * @see #isRemoteJvm()
     * @see #isRemoteJvm(int)
     * @see #isRemoteJvm(String)
     * @see #executeOnLocalOrRemoteJvm(int, TestIgniteIdxCallable)
     * @see #executeOnLocalOrRemoteJvm(Ignite, TestIgniteCallable)
     * @see #executeOnLocalOrRemoteJvm(IgniteCache, TestCacheCallable)
     */
    protected boolean isMultiJvm() {
        return false;
    }

    /**
     * @param gridName Grid name.
     * @return {@code True} if the name of the grid indicates that it was the first started (on this JVM).
     */
    protected boolean isFirstGrid(String gridName) {
        return "0".equals(gridName.substring(getTestGridName().length()));
    }

    /**
     * @param gridName Grid name.
     * @return <code>True</code> if test was run in multi-JVM mode and grid with this name was started at another JVM.
     */
    protected boolean isRemoteJvm(String gridName) {
        return isMultiJvm() && !isFirstGrid(gridName);
    }

    /**
     * @param idx Grid index.
     * @return <code>True</code> if test was run in multi-JVM mode and grid with this ID was started at another JVM.
     */
    protected boolean isRemoteJvm(int idx) {
        return isMultiJvm() && idx != 0;
    }

    /**
     * @return <code>True</code> if current JVM contains remote started node
     * (It differs from JVM where tests executing).
     */
    protected boolean isRemoteJvm() {
        return IgniteNodeRunner.hasStartedInstance();
    }

    /**
     * @param cache Cache.
     * @return <code>True</code> if cache is an instance of {@link IgniteCacheProcessProxy}
     */
    public static boolean isMultiJvmObject(IgniteCache cache) {
        return cache instanceof IgniteCacheProcessProxy;
    }

    /**
     * @param ignite Ignite.
     * @return <code>True</code> if cache is an instance of {@link IgniteProcessProxy}
     */
    public static boolean isMultiJvmObject(Ignite ignite) {
        return ignite instanceof IgniteProcessProxy;
    }

    /**
     * Calls job on local JVM or on remote JVM in multi-JVM case.
     *
     * @param idx Grid index.
     * @param job Job.
     */
    public <R> R executeOnLocalOrRemoteJvm(final int idx, final TestIgniteIdxCallable<R> job) {
        IgniteEx ignite = grid(idx);

        if (!isMultiJvmObject(ignite))
            try {
                job.setIgnite(ignite);

                return job.call(idx);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        else
            return executeRemotely(idx, job);
    }

    /**
     * Calls job on local JVM or on remote JVM in multi-JVM case.
     *
     * @param ignite Ignite.
     * @param job Job.
     */
    public static <R> R executeOnLocalOrRemoteJvm(Ignite ignite, final TestIgniteCallable<R> job) {
        if (!isMultiJvmObject(ignite))
            try {
                return job.call(ignite);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        else
            return executeRemotely((IgniteProcessProxy)ignite, job);
    }

    /**
     * Calls job on local JVM or on remote JVM in multi-JVM case.
     *
     * @param cache Cache.
     * @param job Job.
     */
    public static <K,V,R> R executeOnLocalOrRemoteJvm(IgniteCache<K,V> cache, TestCacheCallable<K,V,R> job) {
        Ignite ignite = cache.unwrap(Ignite.class);

        if (!isMultiJvmObject(ignite))
            try {
                return job.call(ignite, cache);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        else
            return executeRemotely((IgniteCacheProcessProxy<K, V>)cache, job);
    }

    /**
     * Calls job on remote JVM.
     *
     * @param idx Grid index.
     * @param job Job.
     */
    public <R> R executeRemotely(final int idx, final TestIgniteIdxCallable<R> job) {
        IgniteEx ignite = grid(idx);

        if (!isMultiJvmObject(ignite))
            throw new IllegalArgumentException("Ignite have to be process proxy.");

        IgniteProcessProxy proxy = (IgniteProcessProxy)ignite;

        return proxy.remoteCompute().call(new ExecuteRemotelyTask<>(job, idx));
    }

    /**
     * Calls job on remote JVM.
     *
     * @param proxy Ignite.
     * @param job Job.
     */
    public static <R> R executeRemotely(IgniteProcessProxy proxy, final TestIgniteCallable<R> job) {
        return proxy.remoteCompute().call(new TestRemoteTask<>(proxy.getId(), job));
    }

    /**
     * Runs job on remote JVM.
     *
     * @param cache Cache.
     * @param job Job.
     */
    public static <K, V, R> R executeRemotely(IgniteCacheProcessProxy<K, V> cache,
        final TestCacheCallable<K, V, R> job) {
        IgniteProcessProxy proxy = (IgniteProcessProxy)cache.unwrap(Ignite.class);

        final UUID id = proxy.getId();
        final String cacheName = cache.getName();

        return proxy.remoteCompute().call(new IgniteCallable<R>() {
            private static final long serialVersionUID = -3868429485920845137L;

            @Override public R call() throws Exception {
                Ignite ignite = Ignition.ignite(id);
                IgniteCache<K,V> cache = ignite.cache(cacheName);

                return job.call(ignite, cache);
            }
        });
    }

    /**
     * @return Test counters.
     */
    protected synchronized TestCounters getTestCounters() throws IgniteCheckedException {
        TestCounters tc = tests.get(getClass());

        if (tc == null)
            tests.put(getClass(), tc = new TestCounters());

        return tc;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ProhibitedExceptionDeclared"})
    @Override protected void runTest() throws Throwable {
        final AtomicReference<Throwable> ex = new AtomicReference<>();

        Thread runner = new IgniteThread(getTestGridName(), "test-runner", new Runnable() {
            @Override public void run() {
                try {
                    runTestInternal();
                }
                catch (Throwable e) {
                    IgniteClosure<Throwable, Throwable> hnd = errorHandler();

                    ex.set(hnd != null ? hnd.apply(e) : e);
                }
            }
        });

        runner.start();

        runner.join(isDebug() ? 0 : getTestTimeout());

        if (runner.isAlive()) {
            U.error(log,
                "Test has been timed out and will be interrupted (threads dump will be taken before interruption) [" +
                "test=" + getName() + ", timeout=" + getTestTimeout() + ']');

            List<Ignite> nodes = IgnitionEx.allGridsx();

            for (Ignite node : nodes)
                ((IgniteKernal)node).dumpDebugInfo();

            // We dump threads to stdout, because we can loose logs in case
            // the build is cancelled on TeamCity.
            U.dumpThreads(null);

            U.dumpThreads(log);

            U.interrupt(runner);

            U.join(runner, log);

            throw new TimeoutException("Test has been timed out [test=" + getName() + ", timeout=" +
                getTestTimeout() + ']' );
        }

        Throwable t = ex.get();

        if (t != null) {
            U.error(log, "Test failed.", t);

            throw t;
        }

        assert !stopGridErr : "Error occurred on grid stop (see log for more details).";
    }

    /**
     * @return Error handler to process all uncaught exceptions of the test run
     *      ({@code null} by default).
     */
    protected IgniteClosure<Throwable, Throwable> errorHandler() {
        return null;
    }

    /**
     * Force test failure.
     *
     * @param msg Message.
     */
    public void forceFailure(@Nullable String msg) {
        forceFailure = true;

        forceFailureMsg = msg;
    }

    /**
     * Set test count.
     */
    public void forceTestCount(int cnt) {
        testCnt = cnt;

        forceTestCnt = true;
    }

    /**
     * @throws Throwable If failed.
     */
    @SuppressWarnings({"ProhibitedExceptionDeclared"})
    private void runTestInternal() throws Throwable {
        if (forceFailure)
            fail("Forced failure: " + forceFailureMsg);
        else
            super.runTest();
    }

    /**
     * @return Test case timeout.
     */
    protected long getTestTimeout() {
        return getDefaultTestTimeout();
    }

    /**
     * @return Default test case timeout.
     */
    private long getDefaultTestTimeout() {
        String timeout = GridTestProperties.getProperty("test.timeout");

        if (timeout != null)
            return Long.parseLong(timeout);

        return DFLT_TEST_TIMEOUT;
    }

    /**
     * @param store Store.
     */
    protected <T> Factory<T> singletonFactory(T store) {
        return notSerializableProxy(new FactoryBuilder.SingletonFactory<T>(store), Factory.class);
    }

    /**
     * @param obj Object that should be wrap proxy
     * @return Created proxy.
     */
    protected <T> T notSerializableProxy(final T obj) {
        Class<T> cls = (Class<T>)obj.getClass();

        Class<T>[] interfaces = (Class<T>[])cls.getInterfaces();

        assert interfaces.length > 0;

        Class<T> lastItf = interfaces[interfaces.length - 1];

        return notSerializableProxy(obj, lastItf, Arrays.copyOf(interfaces, interfaces.length - 1));
    }

    /**
     * @param obj Object that should be wrap proxy
     * @param itfCls Interface that should be implemented by proxy
     * @param itfClses Interfaces that should be implemented by proxy (vararg parameter)
     * @return Created proxy.
     */
    protected <T> T notSerializableProxy(final T obj, Class<? super T> itfCls, Class<? super T> ... itfClses) {
        Class<?>[] itfs = Arrays.copyOf(itfClses, itfClses.length + 3);

        itfs[itfClses.length] = itfCls;
        itfs[itfClses.length + 1] = Serializable.class;
        itfs[itfClses.length + 2] = WriteReplaceOwner.class;

        return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), itfs, new InvocationHandler() {
            @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                if ("writeReplace".equals(mtd.getName()) && mtd.getParameterTypes().length == 0)
                    return supressSerialization(proxy);

                return mtd.invoke(obj, args);
            }
        });
    }

    /**
     * Returns an object that should be returned from writeReplace() method.
     *
     * @param obj Object that must not be changed after serialization/deserialization.
     * @return An object to return from writeReplace()
     */
    private Object supressSerialization(Object obj) {
        SerializableProxy res = new SerializableProxy(UUID.randomUUID());

        serializedObj.put(res.uuid, obj);

        return res;
    }

    /**
     * @param name Name.
     * @param remote Remote.
     * @param thisRemote This remote.
     */
    public static IgniteEx grid(String name, boolean remote, boolean thisRemote) {
        if (!remote)
            return (IgniteEx)G.ignite(name);
        else {
            if (thisRemote)
                return IgniteNodeRunner.startedInstance();
            else
                return IgniteProcessProxy.ignite(name);
        }
    }

    /**
     *
     */
    private static interface WriteReplaceOwner {
        /**
         *
         */
        Object writeReplace();
    }

    /**
     *
     */
    private static class SerializableProxy implements Serializable {
        /** */
        private final UUID uuid;

        /**
         * @param uuid Uuid.
         */
        private SerializableProxy(UUID uuid) {
            this.uuid = uuid;
        }

        /**
         *
         */
        protected Object readResolve() throws ObjectStreamException {
            Object res = serializedObj.get(uuid);

            assert res != null;

            return res;
        }
    }

    /**
     * Remote computation task.
     */
    private static class TestRemoteTask<R> implements IgniteCallable<R> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Node ID. */
        private final UUID id;

        /** Job. */
        private final TestIgniteCallable<R> job;

        /**
         * @param id Id.
         * @param job Job.
         */
        public TestRemoteTask(UUID id, TestIgniteCallable<R> job) {
            this.id = id;
            this.job = job;
        }

        /** {@inheritDoc} */
        @Override public R call() throws Exception {
            Ignite ignite = Ignition.ignite(id);

            return job.call(ignite);
        }
    }

    /**
     *
     */
    private static class ExecuteRemotelyTask<R> implements IgniteCallable<R> {
        /** Ignite. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** Job. */
        private final TestIgniteIdxCallable<R> job;

        /** Index. */
        private final int idx;

        /**
         * @param job Job.
         * @param idx Index.
         */
        public ExecuteRemotelyTask(TestIgniteIdxCallable<R> job, int idx) {
            this.job = job;
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override public R call() throws Exception {
            job.setIgnite(ignite);

            return job.call(idx);
        }
    }

    /**
     * Test counters.
     */
    protected class TestCounters {
        /** */
        private int numOfTests = -1;

        /** */
        private int started;

        /** */
        private int stopped;

        /** */
        private boolean reset;

        /** */
        private IgniteTestResources rsrcs;

        /**
         * @throws IgniteCheckedException In case of error.
         */
        public TestCounters() throws IgniteCheckedException {
            rsrcs = new IgniteTestResources();
        }

        /**
         * @return Reset flag.
         */
        public boolean isReset() {
            return reset;
        }

        /**
         * @return Test resources.
         */
        public IgniteTestResources getTestResources() {
            return rsrcs;
        }

        /**
         * @param reset Reset flag.
         */
        public void setReset(boolean reset) {
            this.reset = reset;
        }

        /** */
        public void reset() {
            numOfTests = -1;
            started = 0;
            stopped = 0;
            reset = false;
        }

        /**
         * @return Started flag.
         */
        public int getStarted() {
            return started;
        }

        /**
         * @param started Started flag.
         */
        public void setStarted(int started) {
            this.started = started;
        }

        /**
         * @return Stopped flag.
         */
        public int getStopped() {
            return stopped;
        }

        /**
         * @param stopped Stopped flag.
         */
        public void setStopped(int stopped) {
            this.stopped = stopped;
        }

        /** */
        public void incrementStarted() {
            if (isDebug())
                info("Incrementing started tests counter.");

            started++;
        }

        /** */
        public void incrementStopped() {
            if (isDebug())
                info("Incrementing stopped tests counter.");

            stopped++;
        }

        /**
         * @return Number of tests
         */
        public int getNumberOfTests() {
            if (numOfTests == -1) {
                GridAbstractTest this0 = GridAbstractTest.this;

                int cnt;

                if (this0.forceTestCnt)
                    cnt = this0.testCnt;
                else {
                    cnt = 0;

                    for (Method m : this0.getClass().getMethods())
                        if (m.getName().startsWith("test") && Modifier.isPublic(m.getModifiers()))
                            cnt++;
                }

                numOfTests = cnt;
            }

            countTestCases();

            return numOfTests;
        }
    }

    /** */
    public static interface TestIgniteCallable<R> extends Serializable {
        /**
         * @param ignite Ignite.
         */
        R call(Ignite ignite) throws Exception;
    }

    /** */
    public abstract static class TestIgniteRunnable implements TestIgniteCallable<Object> {
        /** {@inheritDoc} */
        @Override public Object call(Ignite ignite) throws Exception {
            run(ignite);

            return null;
        }

        /**
         * @param ignite Ignite.
         */
        public abstract void run(Ignite ignite) throws Exception;
    }

    /** */
    public static abstract class TestIgniteIdxCallable<R> implements Serializable {
        @IgniteInstanceResource
        protected Ignite ignite;

        /**
         * @param ignite Ignite.
         */
        public void setIgnite(Ignite ignite) {
            this.ignite = ignite;
        }

        /**
         * @param idx Grid index.
         */
        protected abstract R call(int idx) throws Exception;
    }

    /** */
    public abstract static class TestIgniteIdxRunnable extends TestIgniteIdxCallable<Void> {
        /** {@inheritDoc} */
        @Override public Void call(int idx) throws Exception {
            run(idx);

            return null;
        }

        /**
         * @param idx Index.
         */
        public abstract void run(int idx) throws Exception;
    }

    /** */
    public static interface TestCacheCallable<K, V, R> extends Serializable {
        /**
         * @param ignite Ignite.
         * @param cache Cache.
         */
        R call(Ignite ignite, IgniteCache<K, V> cache) throws Exception;
    }

    /** */
    public abstract static class TestCacheRunnable<K, V> implements TestCacheCallable<K, V, Object> {
        /** {@inheritDoc} */
        @Override public Object call(Ignite ignite, IgniteCache cache) throws Exception {
            run(ignite, cache);

            return null;
        }

        /**
         * @param ignite Ignite.
         * @param cache Cache.
         */
        public abstract void run(Ignite ignite, IgniteCache<K, V> cache) throws Exception;
    }
}
