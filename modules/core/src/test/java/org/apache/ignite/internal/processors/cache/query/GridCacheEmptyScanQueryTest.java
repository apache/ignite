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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.test.ignite2190.Employee;
import org.apache.ignite.test.ignite2190.EmployeePredicate;
import org.apache.ignite.test.ignite2190.ObjectPredicate;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 */
public class GridCacheEmptyScanQueryTest extends GridCommonAbstractTest {
    /** jar name contains Employee, ObjectPredicate, EmployeePredicate classes. */
    private static final String IGNITE_2190_1_0_JAR = "ignite-2190-1.0.jar";

    /** flag to exclude ignite-2190-1.0.jar from remote classpath or exclude test package in grid config */
    public static final String IGNITE_2190_EXCL_FROM_CP = "ignite2190.exclude.from.cp";

    public static final Boolean EXCL_FROM_CP = true;
    public static final Boolean EXCL_FROM_CFG = false;

    /** {@inheritdoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        if (EXCL_FROM_CFG == Boolean.valueOf(System.getProperty(IGNITE_2190_EXCL_FROM_CP, "true")))
            cfg.setPeerClassLoadingLocalClassPathExclude("org.apache.ignite.test.*");

        cfg.setClientMode(false);
        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /**
     * Runs empty ScanQuery with OptimizedMarshaller.
     * ignite-2190-1.0.jar excluded from remote grid classpath
     * @throws Exception If failed.
     */
    public void testEmptyScanQueryWithOptimizedMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(), 2,
            "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller", EXCL_FROM_CP);
    }

    /**
     * Runs empty ScanQuery with BinaryMarshaller.
     * ignite-2190-1.0.jar excluded from remote grid classpath
     * @throws Exception If failed.
     */
    public void testEmptyScanQueryWithBinaryMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(), 2,
            "org.apache.ignite.internal.binary.BinaryMarshaller", EXCL_FROM_CP);
    }

    /**
     * Runs ScanQuery with (Object, Object) predicate and OptimizedMarshaller.
     * ignite-2190-1.0.jar excluded from remote grid classpath
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery2WithOptimizedMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(new ObjectPredicate()), 2,
            "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller", EXCL_FROM_CP);
    }

    /**
     * Runs ScanQuery with (Object, Object) predicate and BinaryMarshaller.
     * ignite-2190-1.0.jar excluded from remote grid classpath
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery2WithBinaryMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<Integer, Employee>(new ObjectPredicate()), 2,
            "org.apache.ignite.internal.binary.BinaryMarshaller", EXCL_FROM_CP);
    }

    /**
     * Runs ScanQuery with (Integer, Employee) predicate and OptimizedMarshaller.
     * ignite-2190-1.0.jar excluded from remote grid classpath
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery3WithOptimizedMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<>(new EmployeePredicate()), 2,
            "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller", EXCL_FROM_CP);
    }

    /**
     * Runs ScanQuery with (Integer, Employee) predicate and BinaryMarshaller.
     * ignite-2190-1.0.jar excluded from remote grid classpath
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery3WithBinaryMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<>(new EmployeePredicate()), 2,
            "org.apache.ignite.internal.binary.BinaryMarshaller", EXCL_FROM_CP);
    }

    /**
     * Runs ScanQuery with (Integer, Employee) predicate and OptimizedMarshaller
     * ignite-2190-1.0.jar available on remote grid
     * cfg.PeerClassLoadingLocalClassPathExclude = org.apache.ignite.test.*
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery4WithOptimizedMarshaller() throws Exception {
        runEmptyScanQuery(new ScanQuery<>(new EmployeePredicate()), 2,
                          "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller", EXCL_FROM_CFG);
    }


    /**
     * Runs ScanQuery with (Integer, Employee) predicate and BinaryMarshaller.
     * ignite-2190-1.0.jar available on remote grid
     * cfg.PeerClassLoadingLocalClassPathExclude = org.apache.ignite.test.*
     * @throws Exception If failed.
     */
    public void testEmptyScanQuery4WithBinaryMarshaller() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5659");
        runEmptyScanQuery(new ScanQuery<>(new EmployeePredicate()), 2,
                          "org.apache.ignite.internal.binary.BinaryMarshaller", EXCL_FROM_CFG);
    }

    /**
     * Runs specified ScanQuery using <code>marsh</code>, check it return <code>expSz</code> record.
     * @throws Exception If failed.
     */
    public void runEmptyScanQuery(ScanQuery<Integer, Employee> qry, int expSz, String marsh, boolean exclFromCp) throws Exception {
        System.setProperty(IGNITE_2190_EXCL_FROM_CP, Boolean.toString(exclFromCp));
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, marsh);
        Ignite local = startGrid(0);
        Ignite remote = startGrid(1);
        stopGrid(0);

        Ignition.setClientMode(true);
        try (Ignite client = Ignition.start(getLocalConfiguration())) {
            CacheConfiguration<Integer, Employee> cacheCfg = new CacheConfiguration<>("testCache");

            IgniteCache<Integer, Employee> cache = client.getOrCreateCache(cacheCfg);

            cache.put(1, new Employee(1, "name 1"));
            cache.put(2, new Employee(2, "name 2"));

            assertEquals("Size of result of always true ScanQuery should be 2", expSz,
                cache.query(qry).getAll().size());
        }
        finally {
            stopAllGrids();
        }
    }

    /** Return configuration for grid running in same jvm. */
    private IgniteConfiguration getLocalConfiguration() throws Exception {
        final IgniteConfiguration cfg = getConfiguration();
        cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteProcessProxy igniteProcessProxy(IgniteConfiguration cfg, Ignite locNode,
        boolean resetDiscovery)
        throws Exception {
        return new IgniteProcessProxy(cfg, log, grid(0)) {
            /** {@inheritDoc} */
            @Override protected Collection<String> filteredJvmArgs(IgniteConfiguration cfg) {
                Collection<String> defaultFilteredJvmArgs = super.filteredJvmArgs(cfg);
                List<String> filteredJvmArgs = new ArrayList<>();

                boolean classpathFound = false;
                Iterator<String> iDefaultFilteredJvmArgs = defaultFilteredJvmArgs.iterator();
                while (iDefaultFilteredJvmArgs.hasNext()) {
                    String arg = iDefaultFilteredJvmArgs.next();

                    if ("-cp".equals(arg) || "-classpath".equals(arg)) {
                        filteredJvmArgs.add(arg);
                        filteredJvmArgs.add(excludeIgnite2190JarFromClasspath(iDefaultFilteredJvmArgs.next()));
                        classpathFound = true;
                    } else
                        filteredJvmArgs.add(arg);
                }

                if (!classpathFound) {
                    String classpath = System.getProperty("java.class.path");
                    String sfcp = System.getProperty("surefire.test.class.path");

                    if (sfcp != null)
                        classpath += System.getProperty("path.separator") + sfcp;

                    filteredJvmArgs.add("-cp");
                    filteredJvmArgs.add(excludeIgnite2190JarFromClasspath(classpath));
                }

                return filteredJvmArgs;
            }

            /** Excluding ignite-2190-1.0.jar so Employee class become invisible for a remote node. */
            String excludeIgnite2190JarFromClasspath(String classpath) {
                if (!Boolean.valueOf(System.getProperty(IGNITE_2190_EXCL_FROM_CP, "true")))
                    return classpath;

                final String pathSeparator = System.getProperty("path.separator");
                final String[] classpathArr = classpath.split(pathSeparator);

                classpath = "";

                for (String aClasspathArr : classpathArr) {
                    if (!aClasspathArr.contains(IGNITE_2190_1_0_JAR))
                        classpath += (classpath.length() > 0 ? pathSeparator : "") + aClasspathArr;
                }

                return classpath;
            }
        };
    }
}
