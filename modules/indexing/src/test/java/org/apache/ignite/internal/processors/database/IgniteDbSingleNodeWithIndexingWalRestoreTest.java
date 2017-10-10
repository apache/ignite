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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Test verifies that binary metadata of values stored in cache and indexes upon these values
 * is handled correctly on cluster restart when persistent store is enabled and compact footer is turned on.
 */
public class IgniteDbSingleNodeWithIndexingWalRestoreTest extends GridCommonAbstractTest {
    /** */
    private static final String BINARY_TYPE_NAME = "BinaryPerson";

    /** */
    private static final String BINARY_TYPE_FIELD_NAME = "binaryName";

    /** */
    private static int ENTRIES_COUNT = 500;

    /** */
    private static class RegularPerson {
        /** */
        private String regName;

        /** */
        public RegularPerson(String regName) {
            this.regName = regName;
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryConfiguration binCfg = new BinaryConfiguration();
        binCfg.setCompactFooter(true);

        cfg.setBinaryConfiguration(binCfg);

        CacheConfiguration indexedCacheCfg = new CacheConfiguration();

        indexedCacheCfg.setName("indexedCache");

        List<QueryEntity> qryEntities = new ArrayList<>();

        {
            QueryEntity qryEntity = new QueryEntity();
            qryEntity.setKeyType(Integer.class.getName());
            qryEntity.setValueType(BINARY_TYPE_NAME);

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put(BINARY_TYPE_FIELD_NAME, String.class.getName());

            qryEntity.setFields(fields);

            qryEntity.setIndexes(F.asList(new QueryIndex(BINARY_TYPE_FIELD_NAME)));

            qryEntities.add(qryEntity);
        }

        {
            QueryEntity qryEntity = new QueryEntity();
            qryEntity.setKeyType(Integer.class.getName());
            qryEntity.setValueType(RegularPerson.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("regName", String.class.getName());

            qryEntity.setFields(fields);

            qryEntity.setIndexes(F.asList(new QueryIndex("regName")));

            qryEntities.add(qryEntity);
        }

        indexedCacheCfg.setQueryEntities(qryEntities);

        cfg.setCacheConfiguration(indexedCacheCfg);

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /**
     * Test for values without class created with BinaryObjectBuilder.
     */
    public void testClasslessBinaryValuesRestored() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) ig.context().cache().context().database();

        dbMgr.enableCheckpoints(false).get();

        IgniteCache<Object, Object> cache = ig.cache("indexedCache").withKeepBinary();

        IgniteBinary bin = ig.binary();

        for (int i = 0; i < ENTRIES_COUNT; i++) {
            BinaryObjectBuilder bldr = bin.builder(BINARY_TYPE_NAME);

            bldr.setField(BINARY_TYPE_FIELD_NAME, "Peter" + i);

            cache.put(i, bldr.build());
        }

        stopGrid(0, true);

        ig = startGrid(0);

        ig.active(true);

        cache = ig.cache("indexedCache").withKeepBinary();

        for (int i = 0; i < ENTRIES_COUNT; i++)
            assertEquals("Peter" + i, (((BinaryObject)cache.get(i)).field(BINARY_TYPE_FIELD_NAME)));
    }

    /**
     * Test for regular objects stored in cache with compactFooter=true setting
     * (no metainformation to deserialize values is stored with values themselves).
     */
    public void testRegularClassesRestored() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager) ig.context().cache().context().database();

        dbMgr.enableCheckpoints(false).get();

        IgniteCache<Object, Object> cache = ig.cache("indexedCache");

        for (int i = 0; i < ENTRIES_COUNT; i++)
            cache.put(i, new RegularPerson("RegularPeter" + i));

        stopGrid(0, true);

        ig = startGrid(0);

        ig.active(true);

        cache = ig.cache("indexedCache");

        for (int i = 0; i < ENTRIES_COUNT; i++)
            assertEquals("RegularPeter" + i, ((RegularPerson)cache.get(i)).regName);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}
