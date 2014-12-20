/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.log4j.*;
import org.apache.log4j.varia.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.logger.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Common stuff for cache load tests.
 */
abstract class GridCacheAbstractLoadTest {
    /** Random. */
    protected static final Random RAND = new Random();

    /** Default configuration file path. */
    protected static final String CONFIG_FILE = "modules/tests/config/spring-cache-load.xml";

    /** Default log file path. */
    protected static final String LOG_FILE = "cache-load.log";

    /** Whether to use transactions. */
    protected final boolean tx;

    /** Operations per transaction. */
    protected final int operationsPerTx;

    /** Transaction isolation level. */
    protected final GridCacheTxIsolation isolation;

    /** Transaction concurrency control. */
    protected final GridCacheTxConcurrency concurrency;

    /** Threads count. */
    protected final int threads;

    /** Write ratio. */
    protected final double writeRatio;

    /** Test duration. */
    protected final long testDuration;

    /** Value size. */
    protected final int valSize;

    /** */
    protected static final int WRITE_LOG_MOD = 100;

    /** */
    protected static final int READ_LOG_MOD = 1000;

    /** Reads. */
    protected final AtomicLong reads = new AtomicLong();

    /** Reads. */
    protected final AtomicLong readTime = new AtomicLong();

    /** Writes. */
    protected final AtomicLong writes = new AtomicLong();

    /** Writes. */
    protected final AtomicLong writeTime = new AtomicLong();

    /** Done flag. */
    protected final AtomicBoolean done = new AtomicBoolean();

    /** */
    protected GridCacheAbstractLoadTest() {
        Properties props = new Properties();

        try {
            props.load(new FileReader(GridTestUtils.resolveGridGainPath(
                "modules/tests/config/cache-load.properties")));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        tx = Boolean.valueOf(props.getProperty("transactions"));
        operationsPerTx = Integer.valueOf(props.getProperty("operations.per.tx"));
        isolation = GridCacheTxIsolation.valueOf(props.getProperty("isolation"));
        concurrency = GridCacheTxConcurrency.valueOf(props.getProperty("concurrency"));
        threads = Integer.valueOf(props.getProperty("threads"));
        writeRatio = Double.valueOf(props.getProperty("write.ratio"));
        testDuration = Long.valueOf(props.getProperty("duration"));
        valSize = Integer.valueOf(props.getProperty("value.size"));
    }

    /**
     * @param writeClos Write closure.
     * @param readClos ReadClosure.
     */
    protected void loadTest(final CIX1<GridCacheProjection<Integer, Integer>> writeClos,
        final CIX1<GridCacheProjection<Integer, Integer>> readClos) {
        info("Read threads: " + readThreads());
        info("Write threads: " + writeThreads());
        info("Test duration (ms): " + testDuration);

        Ignite ignite = G.ignite();

        final GridCache<Integer, Integer> cache = ignite.cache(null);

        assert cache != null;

        try {
            IgniteFuture<?> f1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    long start = System.currentTimeMillis();

                    while (!done.get()) {
                        if (tx) {
                            try (GridCacheTx tx = cache.txStart()) {
                                writeClos.apply(cache);

                                tx.commit();
                            }
                        }
                        else
                            writeClos.apply(cache);
                    }

                    writeTime.addAndGet(System.currentTimeMillis() - start);

                    return null;
                }
            }, writeThreads(), "cache-load-test-worker");

            IgniteFuture<?> f2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    long start = System.currentTimeMillis();

                    while(!done.get()) {
                        if (tx) {
                            try (GridCacheTx tx = cache.txStart()) {
                                readClos.apply(cache);

                                tx.commit();
                            }
                        }
                        else
                            readClos.apply(cache);
                    }

                    readTime.addAndGet(System.currentTimeMillis() - start);

                    return null;
                }
            }, readThreads(), "cache-load-test-worker");

            Thread.sleep(testDuration);

            done.set(true);

            f1.get();
            f2.get();

            info("Test stats: ");
            info("    total-threads = " + threads);
            info("    write-ratio = " + writeRatio);
            info("    total-runs = " + (reads.get() + writes.get()));
            info("    total-reads = " + reads);
            info("    total-writes = " + writes);
            info("    read-time (ms) = " + readTime);
            info("    write-time (ms) = " + writeTime);
            info("    avg-read-time (ms) = " + ((double)readTime.get() / reads.get()));
            info("    avg-write-time (ms) = " + ((double)writeTime.get() / writes.get()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return Write threads count.
     */
    @SuppressWarnings({"ConstantConditions"})
    protected int writeThreads() {
        int ratio = (int)(threads * writeRatio);

        return writeRatio == 0 ? 0 : ratio == 0 ? 1 : ratio;
    }

    /**
     * @return Read threads count.
     */
    @SuppressWarnings({"ConstantConditions"})
    protected int readThreads() {
        int ratio = (int)(threads * (1 - writeRatio));

        return Double.compare(writeRatio, 1) == 0 ? 0 : ratio == 0 ? 1 : ratio;
    }

    /**
     * @param msg Message to print.
     */
    protected static void info(String msg) {
        System.out.println(msg);
    }

    /**
     * @param msg Message to print.
     */
    protected static void error(String msg) {
        System.err.println(msg);
    }

    /**
     * Initializes logger.
     *
     * @param log Log file name.
     * @return Logger.
     * @throws IgniteCheckedException If file initialization failed.
     */
    protected IgniteLogger initLogger(String log) throws IgniteCheckedException {
        Logger impl = Logger.getRootLogger();

        impl.removeAllAppenders();

        String fileName =  U.getGridGainHome() + "/work/log/" + log;

        // Configure output that should go to System.out
        RollingFileAppender fileApp;

        String fmt = "[%d{ABSOLUTE}][%-5p][%t][%c{1}] %m%n";

        try {
            fileApp = new RollingFileAppender(new PatternLayout(fmt), fileName);

            fileApp.setMaxBackupIndex(0);
            fileApp.setAppend(false);

            // fileApp.rollOver();

            fileApp.activateOptions();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to initialize file appender.", e);
        }

        LevelRangeFilter lvlFilter = new LevelRangeFilter();

        lvlFilter.setLevelMin(Level.DEBUG);

        fileApp.addFilter(lvlFilter);

        impl.addAppender(fileApp);

        // Configure output that should go to System.out
        ConsoleAppender conApp = new ConsoleAppender(new PatternLayout(fmt), ConsoleAppender.SYSTEM_OUT);

        lvlFilter = new LevelRangeFilter();

        lvlFilter.setLevelMin(Level.DEBUG);
        lvlFilter.setLevelMax(Level.INFO);

        conApp.addFilter(lvlFilter);

        conApp.activateOptions();

        impl.addAppender(conApp);

        // Configure output that should go to System.err
        conApp = new ConsoleAppender(new PatternLayout(fmt), ConsoleAppender.SYSTEM_ERR);

        conApp.setThreshold(Level.WARN);

        conApp.activateOptions();

        impl.addAppender(conApp);

        impl.setLevel(Level.INFO);

        //Logger.getLogger("org.gridgain").setLevel(Level.INFO);
        //Logger.getLogger(GridCacheVersionManager.class).setLevel(Level.DEBUG);

        return new GridTestLog4jLogger(false);
    }

    /**
     * Initializes configurations.
     *
     * @param springCfgPath Configuration file path.
     * @param log Log file name.
     * @return Configuration.
     * @throws IgniteCheckedException If fails.
     */
    @SuppressWarnings("unchecked")
    protected IgniteConfiguration configuration(String springCfgPath, String log) throws IgniteCheckedException {
        File path = GridTestUtils.resolveGridGainPath(springCfgPath);

        if (path == null)
            throw new IgniteCheckedException("Spring XML configuration file path is invalid: " + new File(springCfgPath) +
                ". Note that this path should be either absolute path or a relative path to GRIDGAIN_HOME.");

        if (!path.isFile())
            throw new IgniteCheckedException("Provided file path is not a file: " + path);

        // Add no-op logger to remove no-appender warning.
        Appender app = new NullAppender();

        Logger.getRootLogger().addAppender(app);

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(path.toURI().toURL().toString());
        }
        catch (BeansException | MalformedURLException e) {
            throw new IgniteCheckedException("Failed to instantiate Spring XML application context: " + e.getMessage(), e);
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
            throw new IgniteCheckedException("Failed to find a single grid factory configuration in: " + path);

        // Remove previously added no-op logger.
        Logger.getRootLogger().removeAppender(app);

        if (cfgMap.isEmpty())
            throw new IgniteCheckedException("Can't find grid factory configuration in: " + path);
        else if (cfgMap.size() > 1)
            throw new IgniteCheckedException("More than one configuration provided for cache load test: " + cfgMap.values());

        IgniteConfiguration cfg = (IgniteConfiguration)cfgMap.values().iterator().next();

        cfg.setGridLogger(initLogger(log));

        cfg.getTransactionsConfiguration().setDefaultTxIsolation(isolation);
        cfg.getTransactionsConfiguration().setDefaultTxConcurrency(concurrency);

        return cfg;
    }
}
