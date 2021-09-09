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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CleanupIndexTreeCheckpointFailoverTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCorruptedTree() throws Exception {
        cleanPersistenceDir();

        IgniteEx ig = startGrid(0);
        ig.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Key, Value> cfg = new CacheConfiguration<Key, Value>()
            .setIndexedTypes(Key.class, Value.class).setName("test");

        IgniteCache<Key, Value> cache = ig.getOrCreateCache(cfg);

        cache.query(new SqlFieldsQuery("create index myindex on value (a asc)")).getAll();

        for (int i = 0; i < 5000; i++)
            cache.put(new Key(i), new Value(String.valueOf(i), "b" + i));

        ig.context().cache().context().database().wakeupForCheckpoint("test").get();

        cache.query(new SqlFieldsQuery("drop index myindex")).getAll();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ig.context().cache().context()
            .database();

        U.sleep(1000);

        dbMgr.enableCheckpoints(false);

        stopGrid(0, true);

        ig = startGrid(0);

        cache = ig.cache("test");

        for (int i = 0; i < 5000; i += 2)
            cache.remove(new Key(i));

        cache.query(new SqlFieldsQuery("create index myindex on value (a asc)")).getAll();

        for (int i = 0; i < 5000; i++)
            cache.put(new Key(i), new Value(String.valueOf(i), "b" + i));
    }

    /**
     */
    private static class Key {
        /** */
        int id;

        /**
         */
        Key(int id) {
            this.id = id;
        }
    }

    /**
     */
    private static class Value {
        /** */
        @QuerySqlField
        String a;

        /** */
        @QuerySqlField
        String b;

        /**
         */
        Value(String a, String b) {
            this.a = a;
            this.b = b;
        }
    }
}
