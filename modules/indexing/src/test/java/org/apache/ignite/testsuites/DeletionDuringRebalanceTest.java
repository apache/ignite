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
package org.apache.ignite.testsuites;

import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;

/**
 * The test checks the SQL delete operation during a rebalance in progress.
 */
public class DeletionDuringRebalanceTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setSqlConfiguration(
                new SqlConfiguration().setQueryEnginesConfiguration(
                    new IndexingQueryEngineConfiguration()))
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            )
            .setCacheConfiguration(
                simpleCacheConfiguration()
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /**
     * Gets a configuration object for the cahce with default name {@code DEFAULT_CACHE_NAME}.
     *
     * @return Cache configuration object.
     */
    private CacheConfiguration simpleCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setBackups(1);

        QueryEntity entity = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Subscription.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("id", Integer.class.getName());
        fields.put("surname", String.class.getName());
        fields.put("orgId", Integer.class.getName());

        entity.setFields(fields);

        cfg.setQueryEntities(Collections.singleton(
            entity
        ));

        return cfg;
    }

    /**
     * The test starts two nodes, preloads data and redtarts one. During the restarted node is rebalancing, SQL delete
     * operation happens. After the topology got stable, the test checks partition consistency.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSimple() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Subscription> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 20; i++) {
            cache.put(i, new Subscription(i, "Ivanov", i % 10));
        }

        forceCheckpoint();

        stopGrid(1);

        for (int i = 2_000; i < 2_010; i++) {
            cache.put(i, new Subscription(i, "Ivanov", i % 10));
        }

        forceCheckpoint();

        spi(ignite).blockMessages((node, message) -> message instanceof GridDhtPartitionSupplyMessage &&
            ((GridDhtPartitionSupplyMessage)message).groupId() == CU.cacheId(DEFAULT_CACHE_NAME));

        IgniteEx ignite1 = startGrid(1);

        cache.query(new SqlFieldsQuery("delete from Subscription where id = 8"));

        spi(ignite).stopBlock();

        awaitPartitionMapExchange();

        assertNull(cache.get(8));

        assertPartitionsSame(idleVerify(ignite1, DEFAULT_CACHE_NAME));
    }

    /**
     * Class for uploading to cache.
     */
    private class Subscription {
        /** Id. */
        @QuerySqlField
        private Integer id;

        /** Surname */
        @QuerySqlField
        private String surname;

        /** Organization id. */
        @QuerySqlField
        @AffinityKeyMapped
        private Integer orgId;

        /**
         * The constructor.
         *
         * @param id Id.
         * @param surname Surname.
         * @param orgId Organization id.
         */
        Subscription(Integer id, String surname, Integer orgId) {
            this.id = id;
            this.surname = surname;
            this.orgId = orgId;
        }

        /**
         * @return Id.
         */
        public Integer getId() {
            return id;
        }

        /**
         * @param id Id.
         */
        public void setId(Integer id) {
            this.id = id;
        }

        /**
         * @return Surname.
         */
        public String getSurname() {
            return surname;
        }

        /**
         * @param surname Surname.
         */
        public void setSurname(String surname) {
            this.surname = surname;
        }

        /**
         * @return Organization id.
         */
        public Integer getOrgId() {
            return orgId;
        }

        /**
         * @param orgId Organization id.
         */
        public void setOrgId(Integer orgId) {
            this.orgId = orgId;
        }
    }
}
