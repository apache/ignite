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
package org.apache.ignite.internal.processors.cache.index;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class IndexRebuildTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that the index is able to rebuild after index.bin is deleted.
     * <p>
     * The tests runs 4 steps, each in a separate JVM.
     */
    public void testIndexRebuildingScript() throws Exception {
        U.delete(Paths.get("work/"));

        // Start node, load data into cache, stop node.
        runJvm(LoadRun.class.getName());

        // Restart node, check that cache.size() and SELECT COUNT(*) are correct, stop node.
        runJvm(CheckSizeRun.class.getName());

        // Delete index.bin.
        U.delete(Paths.get("work/db/mynode/cache-foo/index.bin"));

        // Restart node, check that cache.size() and SELECT COUNT(*) are correct, stop node.
        // index.bin is rebuilt during this step.
        runJvm(CheckSizeRun.class.getName());

        // Restart node, check that cache.size() and SELECT COUNT(*) are correct, stop node.
        runJvm(CheckSizeRun.class.getName());
    }

    /** */
    private static IgniteConfiguration getCfg() {
        return new IgniteConfiguration()
            .setCacheConfiguration(
                new CacheConfiguration<Long, String>("foo")
                    .setIndexedTypes(Long.class, String.class)
                    .setQueryParallelism(1)
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            )
            .setDiscoverySpi(
                new TcpDiscoverySpi()
                    .setIpFinder(
                        new TcpDiscoveryVmIpFinder()
                            .setAddresses(Collections.singleton("127.0.0.1:47500..47509"))
                    )
            )
            .setWorkDirectory(Paths.get("work").toAbsolutePath().toString())
            .setConsistentId("mynode")
            .setAutoActivationEnabled(false);
    }

    /** */
    private static void checkSize(Ignite ignite) {
        IgniteCache<Long, String> cache = ignite.cache("foo");

        Long cacheSize = cache.sizeLong();
        FieldsQueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select count(*) from String"));
        Long sqlSize = (Long)cursor.iterator().next().get(0);

        ignite.log().error(">>> cacheSize=" + cacheSize + ", sqlSize=" + sqlSize);

        System.out.println(">>> cacheSize=" + cacheSize + ", sqlSize=" + sqlSize);
    }

    /** */
    public static class LoadRun {
        /** */
        public static void main(String[] args) throws InterruptedException {
            try {
                X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());
                System.out.println(">>> Putting data");

                Ignite ignite = Ignition.start(getCfg());
                ignite.cluster().active(true);

                IgniteCache<Object, Object> foo = ignite.cache("foo");
                for (long i = 0; i < 100; i++)
                    foo.put(i, i + "-bar");

                checkSize(ignite);
            }
            finally {
                System.out.println(">>> Finished");
            }
        }
    }

    /** */
    public static class CheckSizeRun {
        /** */
        public static void main(String[] args) throws InterruptedException {
            try {
                X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());
                System.out.println(">>> Checking size");

                Ignite ignite = Ignition.start(getCfg());
                ignite.cluster().active(true);

                checkSize(ignite);
                Thread.sleep(5000);
                checkSize(ignite);
            }
            finally {
                System.out.println(">>> Finished");
            }
        }
    }

    /** */
    private static Collection<String> jvmArgs() throws Exception {
        Collection<String> filteredJvmArgs = new ArrayList<>();

        filteredJvmArgs.add("-ea");

        filteredJvmArgs.add("-D" + IgniteSystemProperties.IGNITE_QUIET + "=false");

        for (String arg : U.jvmArgs()) {
            if (arg.startsWith("-Xmx") || arg.startsWith("-Xms") ||
                arg.startsWith("-cp") || arg.startsWith("-classpath"))
                filteredJvmArgs.add(arg);
        }

        return filteredJvmArgs;
    }

    /** */
    private void runJvm(String clsName) throws Exception {
        final CountDownLatch cdl = new CountDownLatch(1);

        final Pattern ptrn = Pattern.compile(">>> cacheSize=(\\d+), sqlSize=(\\d+)");

        final AtomicBoolean failed = new AtomicBoolean(false);

        GridJavaProcess proc = null;
        try {
            proc = GridJavaProcess.exec(
                clsName,
                "",
                log,
                s -> {
                    log.info(s);

                    if (s.equals(">>> Finished"))
                        cdl.countDown();

                    Matcher m = ptrn.matcher(s);
                    if (m.matches()) {
                        long cacheSize = Long.valueOf(m.group(1));
                        long sqlSize = Long.valueOf(m.group(2));
                        if (cacheSize != sqlSize)
                            failed.set(true);
                    }
                },
                null,
                null,
                jvmArgs(),
                null
            );

            cdl.await();
        }
        finally {
            if (proc != null)
                proc.kill();
        }

        assertFalse(failed.get());
    }
}