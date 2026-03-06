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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/** */
public abstract class PageEvictionPutLargeObjectsAbstractTest extends GridCommonAbstractTest {
    /** Offheap size for memory policy. */
    private static final int SIZE = 90 * 1024 * 1024;

    /** Offheap size for memory policy. */
    private static final long MAX_SIZE = 10L * 1024 * 1024 * 1024;

    /** Record size. */
    private static final int RECORD_SIZE = 24 * 1024 * 1024;

    /** Number of entries. */
    static final int ENTRIES = 50;

    /**
     -Xms15g
     -Xmx15g
     -ea
     -server
     -XX:+ScavengeBeforeFullGC
     -XX:+AlwaysPreTouch
     -XX:+UnlockDiagnosticVMOptions
     -XX:+ExitOnOutOfMemoryError
     -XX:+HeapDumpOnOutOfMemoryError
     -XX:ErrorFile=/home/fmj/tmp/ignite/logs/crash-dump-%p.err
     -XX:HeapDumpPath=/home/fmj/tmp/ignite/logs
     -XX:+PerfDisableSharedMem
     -XX:+UnlockExperimentalVMOptions
     -XX:-OmitStackTraceInFastThrow
     -XX:+LogVMOutput
     -XX:LogFile=/home/fmj/tmp/ignite/logs/hotspot_pid%p.log
     -Dfile.encoding=UTF-8
     -Djava.io.tmpdir=/home/fmj/tmp/ignite/tmp
     -Djava.net.preferIPv4Stack=true
     -Dnetworkaddress.cache.ttl=-1
     -DIGNITE_CLUSTER_NAME=ignite
     -DIGNITE_CLUSTER_TYPE=prom
     -DIGNITE_QUIET=false
     -DIGNITE.DEPLOYMENT.ADDITIONAL.CHECK=true
     -DIGNITE_EXCHANGE_HISTORY_SIZE=100
     -DIGNITE_TO_STRING_INCLUDE_SENSITIVE=true
     -DIGNITE_MBEAN_APPEND_CLASS_LOADER_ID=false
     -DIGNITE_REUSE_MEMORY_ON_DEACTIVATE=true
     -DIGNITE_DISCOVERY_DISABLE_CACHE_METRICS_UPDATE=true
     -DIGNITE_USE_ASYNC_FILE_IO_FACTORY=true
     -DIGNITE_EXCHANGE_HISTORY_SIZE=100
     -DIGNITE_AFFINITY_HISTORY_SIZE=50
     -DIGNITE_ENABLE_EXTRA_INDEX_REBUILD_LOGGING=true
     -DIGNITE_DUMP_THREADS_ON_FAILURE=false
     -DIGNITE_ENABLE_FORCIBLE_NODE_KILL=true
     -DIGNITE_DIAGNOSTIC_ENABLED=false
     -DIGNITE_UPDATE_NOTIFIER=false
     --add-opens
     jdk.management/com.sun.management.internal=ALL-UNNAMED
     -Xlog:gc*=debug,safepoint:file=/home/fmj/tmp/ignite/logs/gc-safepoint.log:time,uptime,tags:filecount=50,filesize=50M
     -Dlog4j2.formatMsgNoLookups=true
     */

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(SIZE)
                    .setMaxSize(MAX_SIZE)
                    .setEmptyPagesPoolSize(1000)
                    .setPersistenceEnabled(false)
                    .setMetricsEnabled(true)
                    .setEvictionThreshold(0.8)
                )
                .setPageSize(DFLT_PAGE_SIZE)
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_DUMP_THREADS_ON_FAILURE", value = "false")
    public void testPutLargeObjects() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        IgniteCache<Integer, Object> cache = ignite.createCache(DEFAULT_CACHE_NAME);

        Random rnd = new Random();

        while(true)
            cache.put(rnd.nextInt(), new TestObject(RECORD_SIZE));
    }
}
