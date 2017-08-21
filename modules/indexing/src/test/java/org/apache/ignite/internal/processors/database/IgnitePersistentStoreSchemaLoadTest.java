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

package org.apache.ignite.internal.processors.database;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;

/**
 *
 */
public class IgnitePersistentStoreSchemaLoadTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Index name. */
    private static final String IDX_NAME = "my_idx";

    /** Cache name. */
    private static final String TMPL_NAME = "test_cache*";

    /** Table name. */
    private static final String TBL_NAME = Person.class.getSimpleName();

    /** Schema name. */
    private static final String SCHEMA_NAME = "PUBLIC";

    /** Cache name. */
    private static final String CACHE_NAME = TBL_NAME;


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setCacheConfiguration(cacheCfg(TMPL_NAME));

        PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();

        pCfg.setCheckpointingFrequency(1000);

        cfg.setPersistentStoreConfiguration(pCfg);

        return cfg;
    }

    /** */
    private CacheConfiguration cacheCfg(String name) {
        CacheConfiguration<?, ?> cfg = new CacheConfiguration<>();

        cfg.setName(name);

        cfg.setRebalanceMode(CacheRebalanceMode.NONE);

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        return cfg;
    }

    /** */
    private QueryEntity getEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", Integer.class.getName());
        fields.put("name", String.class.getName());

        QueryEntity entity = new QueryEntity(Integer.class.getName(), Person.class.getName());
        entity.setFields(fields);
        entity.setTableName(TBL_NAME);

        return entity;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, "true");

        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();

        System.clearProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK);
    }

    /** */
    public void testPersistIndex() throws Exception {
        IgniteEx ig0 = startGrid(0);
        startGrid(1);

        final AtomicInteger cnt = new AtomicInteger();

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig0.context().cache().context().database();

        db.addCheckpointListener(new DbCheckpointListener() {
            @Override public void onCheckpointBegin(Context context) {
                cnt.incrementAndGet();
            }
        });

        QueryIndex idx = new QueryIndex("name");

        idx.setName(IDX_NAME);

        ig0.context().query().dynamicTableCreate(SCHEMA_NAME, getEntity(), TMPL_NAME, null, null, null, 1, true);

        assert indexCnt(ig0, CACHE_NAME) == 0;

        ig0.context().query().dynamicIndexCreate(CACHE_NAME, SCHEMA_NAME, TBL_NAME, idx, false).get();

        assert indexCnt(ig0, CACHE_NAME) == 1;

        waitForCheckpoint(cnt);

        stopGrid(1);

        IgniteEx ig1 = startGrid(1);

        assert indexCnt(ig1, CACHE_NAME) == 1;
    }

    /** */
    public void testPersistCompositeIndex() throws Exception {
        IgniteEx ig0 = startGrid(0);
        startGrid(1);

        final AtomicInteger cnt = new AtomicInteger();

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig0.context().cache().context().database();

        db.addCheckpointListener(new DbCheckpointListener() {
            @Override public void onCheckpointBegin(Context context) {
                cnt.incrementAndGet();
            }
        });

        ig0.context().query().dynamicTableCreate(SCHEMA_NAME, getEntity(), TMPL_NAME, null, null, null, 1, true);

        assert indexCnt(ig0, CACHE_NAME) == 0;

        QueryIndex idx = new QueryIndex(Arrays.asList("id", "name"), QueryIndexType.SORTED);

        idx.setName(IDX_NAME);

        ig0.context().query().dynamicIndexCreate(CACHE_NAME, SCHEMA_NAME, TBL_NAME, idx, false).get();

        assert indexCnt(ig0, CACHE_NAME) == 1;

        waitForCheckpoint(cnt);

        stopGrid(1);

        IgniteEx ig1 = startGrid(1);

        assert indexCnt(ig1, CACHE_NAME) == 1;
    }

    /** */
    private void waitForCheckpoint(final AtomicInteger cnt) throws IgniteInterruptedCheckedException {
        final int i = cnt.get();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cnt.get() > i;
            }
        }, 2000);
    }

    /** */
    private int indexCnt(IgniteEx node, String cacheName) {

        DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

        int cnt = 0;

        for (QueryEntity entity : desc.schema().entities()) {
            cnt += entity.getIndexes().size();
        }

        return cnt;
    }

    /**
     * @return Indexing.
     */
    private IgniteH2Indexing getIndexing(IgniteEx ignite) {
        return U.field(ignite.context().query(), "idx");
    }

    /**
     *
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     *
     */
    protected static class Person implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings("unused")
        private Person() {
            // No-op.
        }

        /** */
        public Person(int id) {
            this.id = id;
        }

        /** */
        @QuerySqlField
        protected int id;

        /** */
        @QuerySqlField
        protected String name;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            IgnitePersistentStoreSchemaLoadTest.Person person = (IgnitePersistentStoreSchemaLoadTest.Person)o;

            if (id != person.id)
                return false;

            return name != null ? name.equals(person.name) : person.name == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;

            result = 31 * result + (name != null ? name.hashCode() : 0);

            return result;
        }
    }
}
