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

package org.apache.ignite.loadtests.cache;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.varia.LevelRangeFilter;
import org.apache.log4j.varia.NullAppender;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

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
    protected final TransactionIsolation isolation;

    /** Transaction concurrency control. */
    protected final TransactionConcurrency concurrency;

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
            props.load(new FileReader(GridTestUtils.resolveIgnitePath(
                    "modules/tests/config/cache-load.properties")));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        tx = Boolean.valueOf(props.getProperty("transactions"));
        operationsPerTx = Integer.valueOf(props.getProperty("operations.per.tx"));
        isolation = TransactionIsolation.valueOf(props.getProperty("isolation"));
        concurrency = TransactionConcurrency.valueOf(props.getProperty("concurrency"));
        threads = Integer.valueOf(props.getProperty("threads"));
        writeRatio = Double.valueOf(props.getProperty("write.ratio"));
        testDuration = Long.valueOf(props.getProperty("duration"));
        valSize = Integer.valueOf(props.getProperty("value.size"));
    }

    /**
     * @param writeClos Write closure.
     * @param readClos ReadClosure.
     */
    protected void loadTest(final CIX1<IgniteCache<Integer, Integer>> writeClos,
        final CIX1<IgniteCache<Integer, Integer>> readClos) {
        info("Read threads: " + readThreads());
        info("Write threads: " + writeThreads());
        info("Test duration (ms): " + testDuration);

        final Ignite ignite = G.ignite();

        final IgniteCache<Integer, Integer> cache = ignite.cache(null);

        assert cache != null;

        try {
            IgniteInternalFuture<?> f1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    long start = System.currentTimeMillis();

                    while (!done.get()) {
                        if (tx) {
                            try (Transaction tx = ignite.transactions().txStart()) {
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

            IgniteInternalFuture<?> f2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    long start = System.currentTimeMillis();

                    while(!done.get()) {
                        if (tx) {
                            try (Transaction tx = ignite.transactions().txStart()) {
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

        String fileName =  U.getIgniteHome() + "/work/log/" + log;

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

        //Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);
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
        File path = GridTestUtils.resolveIgnitePath(springCfgPath);

        if (path == null)
            throw new IgniteCheckedException("Spring XML configuration file path is invalid: " + new File(springCfgPath) +
                ". Note that this path should be either absolute path or a relative path to IGNITE_HOME.");

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

        cfg.getTransactionConfiguration().setDefaultTxIsolation(isolation);
        cfg.getTransactionConfiguration().setDefaultTxConcurrency(concurrency);

        return cfg;
    }
}