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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgnitePersistenceMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String GROUP1 = "grp1";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        MemoryConfiguration memCfg = new MemoryConfiguration();
        memCfg.setPageSize(1024);

        memCfg.setDefaultMemoryPolicyName("dflt-plc");

        MemoryPolicyConfiguration memPlc = new MemoryPolicyConfiguration();
        memPlc.setName("dflt-plc");
        memPlc.setMaxSize(10 * 1024 * 1024);
        memPlc.setMetricsEnabled(true);

        memCfg.setMemoryPolicies(memPlc);

        cfg.setMemoryConfiguration(memCfg);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration()
            .setMetricsEnabled(true).setWalMode(WALMode.LOG_ONLY));

        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(cacheConfiguration(GROUP1, "cache", PARTITIONED, ATOMIC, 1));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        GridTestUtils.deleteDbFiles();

        super.afterTest();
    }

    /**
     * @param grpName Cache group name.
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Backups number.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(
        String grpName,
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testPersistenceMetrics() throws Exception {
        final IgniteEx ig = startGrid(0);

        ig.active(true);

        try {
            IgniteCache<Object, Object> cache = ig.cache("cache");

            for (int i = 0; i < 10; i++)
                cache.put(i, new Person("first-" + i, "last-" + i));

            {
                MemoryMetrics memMetrics = ig.memoryMetrics("dflt-plc");

                assertNotNull(memMetrics);
                assertTrue(memMetrics.getDirtyPages() > 0);
            }

            ig.context().cache().context().database().waitForCheckpoint("test");

            GridTestUtils.waitForCondition(new PAX() {
                @Override public boolean applyx() {
                    PersistenceMetrics pMetrics = ig.persistentStoreMetrics();

                    assertNotNull(pMetrics);

                    return pMetrics.getLastCheckpointTotalPagesNumber() != 0 &&
                        pMetrics.getLastCheckpointDataPagesNumber() != 0;
                }
            }, 5_000);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    static class Person implements Serializable {
        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String fName;

        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String lName;

        /**
         * @param fName First name.
         * @param lName Last name.
         */
        public Person(String fName, String lName) {
            this.fName = fName;
            this.lName = lName;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(fName, person.fName) &&
                Objects.equals(lName, person.lName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(fName, lName);
        }
    }
}
