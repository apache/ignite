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

import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Verifies that if local query is executed on client node and non local cache,
 * exception with reasonable message will be thrown.
 * In case of local cache no exceptions should be thrown.
 */
public class ClientLocalQueryTest extends GridCommonAbstractTest {
    /** Client node. Shared across test methods. */
    private static Ignite client;

    /** Name of created cache. */
    private static final String CACHE_NAME = "TestCache";

    /** Name of local cache. */
    private static final String LOCAL_CAHE_NAME = "TestLocalCache";

    /** Name of region for the local cache. */
    private static final String LOCAL_CACHE_REGION_NAME = "LocalRegion";

    /** Creates new common configuration with the region for the local cache. */
    private IgniteConfiguration newConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = getConfiguration(name);

        // Local caches are created on each cluster node.
        // We need to add configuration for local cache region both for server and client.
        DataStorageConfiguration dsc = new DataStorageConfiguration();
        dsc.setDataRegionConfigurations(
            new DataRegionConfiguration()
                .setName(LOCAL_CACHE_REGION_NAME)
                .setInitialSize(11L *1024 * 1024)
                .setMaxSize(20L * 1024 * 1024));

        cfg.setDataStorageConfiguration(dsc);

        return optimize(cfg);
    }

    /** Sets up grids */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("server", newConfiguration("server"), null);

        IgniteConfiguration clCfg = newConfiguration("client");
        clCfg.setClientMode(true);

        client = startGrid("client", clCfg, null);
    }

    /** Creates cache and test table for the test */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CacheConfiguration<Object, UUID> partCfg = new CacheConfiguration<>(CACHE_NAME);
        partCfg.setIndexedTypes(Object.class, UUID.class);
        partCfg.setCacheMode(CacheMode.PARTITIONED);

        client.createCache(partCfg);

        CacheConfiguration<Object, UUID> locCfg = new CacheConfiguration<>(LOCAL_CAHE_NAME);
        locCfg.setCacheMode(CacheMode.LOCAL);
        locCfg.setDataRegionName(LOCAL_CACHE_REGION_NAME);
        locCfg.setIndexedTypes(Object.class, UUID.class);

        client.createCache(locCfg);

        log.info("Created cache with cfg: " + partCfg);
    }

    /** Destroy the cache. */
    @Override protected void afterTest() throws Exception {
        client.destroyCache(CACHE_NAME);

        client.destroyCache(LOCAL_CAHE_NAME);

        super.afterTest();
    }

    /** Stops all grids. */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** Check for SqlFieldsQuery. */
    public void testLocalSqlFieldsQuery() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT count(_key) FROM UUID;");

        assertCorrectExceptionThrown(qry.setLocal(true));
    }

    /** Check for SqlQuery. */
    public void testLocalSqlQuery() throws Exception {
        SqlQuery<Object, UUID> qry = new SqlQuery<>(UUID.class, "1 = 1");

        assertCorrectExceptionThrown(qry.setLocal(true));
    }

    /** Check for TextQuery. */
    public void testLocalTextQuery() throws Exception {
        TextQuery<Object, UUID> qry = new TextQuery<>(UUID.class, "doesn't matter");

        assertCorrectExceptionThrown(qry.setLocal(true));
    }

    /** Check that it is still possible to perform local queries on local caches. */
    public void testPositiveLocalCache(){
        SqlFieldsQuery locQry = new SqlFieldsQuery("SELECT count(_key) FROM UUID;").setLocal(true);

        client.cache(LOCAL_CAHE_NAME).query(locQry).getAll();
    }

    /** Assert that Exception is thrown. */
    private void assertCorrectExceptionThrown(Query<?> qry) {
        Callable<Void> call = () -> {
            client.cache(CACHE_NAME).query(qry).getAll();
            return null;
        };

        GridTestUtils.assertThrows(log, call, CacheException.class, "Local queries are NOT supported on client nodes");
    }
}
