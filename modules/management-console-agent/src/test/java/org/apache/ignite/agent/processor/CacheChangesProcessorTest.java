/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.processor;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.agent.dto.cache.CacheInfo;
import org.apache.ignite.agent.dto.cache.CacheSqlIndexMetadata;
import org.apache.ignite.agent.dto.cache.CacheSqlMetadata;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterCachesInfoDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterCachesSqlMetaDest;

/**
 * Cache changes processor test.
 */
public class CacheChangesProcessorTest extends AgentCommonAbstractTest {
    /**
     * Should send initial states to backend.
     */
    @Test
    public void shouldSendInitialStates() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        assertWithPoll(() -> interceptor.getPayload(buildClusterCachesInfoDest(cluster.id())) != null);
        assertWithPoll(() -> interceptor.getPayload(buildClusterCachesSqlMetaDest(cluster.id())) != null);
    }

    /**
     * Should not send system cache info.
     */
    @Test
    public void shouldNotSendSystemCacheInfo() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.getOrCreateCache("test-cache");

        assertWithPoll(() -> {
            List<CacheInfo> cachesInfo = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return !cachesInfo.isEmpty() && cachesInfo.stream().noneMatch(i -> "ignite-sys-cache".equals(i.getName()));
        });
    }

    /**
     * Should send correct cache info on create and destroy cache events.
     */
    @Test
    public void shouldSendCacheInfoOnCreatedOrDestroyedCache() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("test-cache");

        cache.put(1, 2);

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "test-cache".equals(i.getName()));
        });

        cache.destroy();

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "test-cache".equals(i.getName()));
        });
    }

    /**
     * Should send correct cache info on create and destroy cache events on other nodes.
     */
    @Test
    public void shouldSendCacheInfoIfCacheCreatedOnOtherNode() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        IgniteEx ignite_2 = startGrid(0);

        IgniteCache<Object, Object> cache = ignite_2.getOrCreateCache("test-cache-1");

        cache.put(1, 2);

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "test-cache-1".equals(i.getName()));
        });

        cache.destroy();

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "test-cache-1".equals(i.getName()));
        });
    }

    /**
     * Should send correct cache info on create and destroy cache events triggered by sql.
     */
    @Test
    public void shouldSendCacheInfoOnCreatedOrDestroyedCacheFromSql() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_1 (id int, value int, PRIMARY KEY (id));"),
            true
        );

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().anyMatch(i -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_1".equals(i.getName()));
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("DROP TABLE mc_agent_test_table_1;"),
            true
        );

        assertWithPoll(() -> {
            List<CacheInfo> cacheInfos = interceptor.getListPayload(buildClusterCachesInfoDest(cluster.id()), CacheInfo.class);

            return cacheInfos != null && cacheInfos.stream().noneMatch(i -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_1".equals(i.getName()));
        });
    }

    /**
     * Should send correct sql metadata.
     */
    @Test
    public void shouldSendCacheMetadataOnAlterTableAndCreateIndex() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_2 (id int, value int, PRIMARY KEY (id));"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_2".equals(m.getCacheName()))
                .findFirst()
                .get();

            Map<String, String> fields = cacheMeta.getFields();

            return cacheMeta != null && fields.containsKey("ID") && fields.containsKey("VALUE");
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("ALTER TABLE mc_agent_test_table_2 ADD id_2 int;"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_2".equals(m.getCacheName()))
                .findFirst()
                .get();

            Map<String, String> fields = cacheMeta.getFields();

            return cacheMeta != null && fields.containsKey("ID") && fields.containsKey("VALUE") && fields.containsKey("ID_2");
        });
    }

    /**
     * Should send correct sql metadata on create and drop index.
     */
    @Test
    public void shouldSendCacheMetadataOnCreateAndDropIndex() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        changeManagementConsoleUri(ignite);

        IgniteCluster cluster = ignite.cluster();

        cluster.active(true);

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE TABLE mc_agent_test_table_3 (id int, value int, PRIMARY KEY (id));"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_3".equals(m.getCacheName()))
                .findFirst()
                .get();

            List<CacheSqlIndexMetadata> idxes = cacheMeta.getIndexes();

            return cacheMeta != null && idxes.isEmpty();
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("CREATE INDEX my_index ON mc_agent_test_table_3 (value)"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_3".equals(m.getCacheName()))
                .findFirst()
                .get();

            List<CacheSqlIndexMetadata> idxes = cacheMeta.getIndexes();

            return cacheMeta != null && idxes.size() == 1;
        });

        ignite.context().query().querySqlFields(
            new SqlFieldsQuery("DROP INDEX my_index;"),
            true
        );

        assertWithPoll(() -> {
            List<CacheSqlMetadata> metadata =
                interceptor.getListPayload(buildClusterCachesSqlMetaDest(cluster.id()), CacheSqlMetadata.class);

            if (F.isEmpty(metadata))
                return false;

            CacheSqlMetadata cacheMeta = metadata.stream()
                .filter(m -> "SQL_PUBLIC_MC_AGENT_TEST_TABLE_3".equals(m.getCacheName()))
                .findFirst()
                .get();

            List<CacheSqlIndexMetadata> idxes = cacheMeta.getIndexes();

            return cacheMeta != null && idxes.isEmpty();
        });
    }
}
