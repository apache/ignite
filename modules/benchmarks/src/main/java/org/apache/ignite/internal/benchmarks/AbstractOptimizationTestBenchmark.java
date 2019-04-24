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
package org.apache.ignite.internal.benchmarks;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public abstract class AbstractOptimizationTestBenchmark {
    /** */
    protected final IgniteLogger log = new BenchmarkLogger();

    /**
     * Cleans persistent directory.
     *
     * @throws Exception if failed.
     */
    protected void cleanPersistenceDir() throws Exception {
        if (!F.isEmpty(G.allGrids()))
            throw new IgniteException("Grids are not stopped");

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /** */
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(
                    new TcpDiscoveryMulticastIpFinder()
                        .setAddresses(Collections.singleton("127.0.0.1:47500..47502"))
                )
        );

        return cfg;
    }

    /**
     * Entrypoint method.
     *
     * @throws Exception if failed.
     */
    protected void benchmark() throws Exception {
        beforeBenchmark();

        beforeTest();

        enableOptimization(false);

        Map<TestResultParameterInfo, Comparable> withoutOptimization = testGrid();

        afterTest();

        beforeTest();

        enableOptimization(true);

        Map<TestResultParameterInfo, Comparable> withOptimization = testGrid();

        afterTest();

        log.info("Before optimization: ");

        withoutOptimization.forEach((k, v) -> {
            log.info(k.name + ": " + v);
        });

        log.info("After optimization: ");

        withOptimization.forEach((k, v) -> {
            log.info(k.name + ": " + v);
        });

        withoutOptimization.forEach((k, deoptimized) -> {
            Comparable optimized = withOptimization.get(k);

            int comparison = optimized.compareTo(deoptimized);

            Integer ratio = null;

            if (optimized instanceof Number && deoptimized instanceof Number) {
                Number o = (Number)optimized;
                Number d = (Number)deoptimized;

                ratio = 100 - new Double(o.doubleValue() / d.doubleValue() * 100).intValue();
            }

            if ((comparison > 0 && k.greaterIsBetter) || (comparison < 0 && !k.greaterIsBetter))
                log.info(k.name + " got better after optimization" +
                    (ratio == null ? "." : String.format(": changed by %d percent", ratio)));
            else
                log.warning(k.name + " got worse after optimization" +
                    (ratio == null ? "." : String.format(": changed by %d percent", ratio)));
        });

        afterBenchmark();
    }

    /**
     * Method to enable or disable optimization for benchmark.
     *
     * @param enable whether enable.
     */
    protected abstract void enableOptimization(boolean enable);

    /**
     * Test grid.
     *
     * @return result map.
     */
    protected abstract Map<TestResultParameterInfo, Comparable> testGrid();

    /**
     * Returns the collection of info objects, containing information about parameters which are measured
     * by this benchmark.
     *
     * @return collection of result parameters info.
     */
    protected abstract Collection<TestResultParameterInfo> testingResults();

    /**
     * This method is executed before whole benchmark.
     *
     * @throws Exception if failed.
     */
    protected void beforeBenchmark() throws Exception {
        /* No-op */
    }

    /**
     * This method is executed after whole benchmark.
     *
     * @throws Exception if failed.
     */
    protected void afterBenchmark() throws Exception {
        /* No-op */
    }

    /**
     * This method is executed before testing each grid.
     *
     * @throws Exception if failed.
     */
    protected void beforeTest() throws Exception {
        /* No-op */
    }

    /**
     * This method is executed after testing each grid.
     *
     * @throws Exception if failed.
     */
    protected void afterTest() throws Exception {
        /* No-op */
    }

    /** */
    private static class BenchmarkLogger implements IgniteLogger {
        /** */
        private volatile boolean quiet;

        /**
         * Default constructor.
         */
        BenchmarkLogger() {
            /* No-op */
        }

        /** {@inheritDoc} */
        @Override public IgniteLogger getLogger(Object ctgr) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void trace(String msg) {
            if (isTraceEnabled())
                System.out.println(msg);
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            if (isDebugEnabled())
                System.out.println(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            if (isInfoEnabled())
                System.out.println(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, @Nullable Throwable e) {
            System.err.println(msg);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable e) {
            System.err.println(msg);
        }

        /** {@inheritDoc} */
        @Override public boolean isTraceEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isInfoEnabled() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean isQuiet() {
            return quiet;
        }

        /** {@inheritDoc} */
        @Override public String fileName() {
            return null;
        }
    }

    /**
     * This class contains info about single parameter, which is measured by benchmark (e.g. heap usage, etc.).
     */
    public static class TestResultParameterInfo {
        /** */
        public final String name;

        /** */
        public final boolean greaterIsBetter;

        /** */
        public TestResultParameterInfo(String name, boolean better) {
            this.name = name;
            greaterIsBetter = better;
        }
    }
}
