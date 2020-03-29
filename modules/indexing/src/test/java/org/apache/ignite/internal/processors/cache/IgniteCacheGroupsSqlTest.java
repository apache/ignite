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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.AffinityKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheGroupsSqlTest extends GridCommonAbstractTest {
    /** */
    private static final String GROUP1 = "grp1";

    /** */
    private static final String GROUP2 = "grp2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(200L * 1024 * 1024)
                ));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlQuery() throws Exception {
        Ignite node = ignite(0);

        IgniteCache c1 = node.createCache(personCacheConfiguration(GROUP1, "c1"));
        IgniteCache c2 = node.createCache(personCacheConfiguration(GROUP1, "c2"));

        SqlFieldsQuery qry = new SqlFieldsQuery("select name from Person where name=?");
        qry.setArgs("p1");

        assertEquals(0, c1.query(qry).getAll().size());
        assertEquals(0, c2.query(qry).getAll().size());

        c1.put(1, new Person("p1"));

        assertEquals(1, c1.query(qry).getAll().size());
        assertEquals(0, c2.query(qry).getAll().size());

        c2.put(2, new Person("p1"));

        assertEquals(1, c1.query(qry).getAll().size());
        assertEquals(1, c2.query(qry).getAll().size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinQuery1() throws Exception {
        joinQuery(GROUP1, GROUP2, REPLICATED, PARTITIONED, TRANSACTIONAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinQuery2() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                joinQuery(GROUP1, GROUP1, REPLICATED, PARTITIONED, TRANSACTIONAL, TRANSACTIONAL);
                return null;
            }
        }, IgniteCheckedException.class, "Cache mode mismatch for caches related to the same group");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinQuery3() throws Exception {
        joinQuery(GROUP1, GROUP1, PARTITIONED, PARTITIONED, TRANSACTIONAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinQuery4() throws Exception {
        joinQuery(GROUP1, GROUP1, REPLICATED, REPLICATED, ATOMIC, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinQuery5() throws Exception {
        joinQuery(GROUP1, null, REPLICATED, PARTITIONED, TRANSACTIONAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinQuery6() throws Exception {
        joinQuery(GROUP1, null, PARTITIONED, PARTITIONED, TRANSACTIONAL, ATOMIC);
    }

    /**
     * @param grp1 First cache group.
     * @param grp2 Second cache group.
     * @param cm1 First cache mode.
     * @param cm2 Second cache mode.
     * @param cam1 First cache atomicity mode.
     * @param cam2 Second cache atomicity mode.
     * @throws Exception If failed.
     */
    private void joinQuery(String grp1, String grp2, CacheMode cm1,
        CacheMode cm2, CacheAtomicityMode cam1, CacheAtomicityMode cam2) throws Exception {
        int keys = 1000;
        int accsPerPerson = 4;

        Ignite srv0 = ignite(0);

        IgniteCache pers = srv0.createCache(personCacheConfiguration(grp1, "pers")
            .setAffinity(new RendezvousAffinityFunction().setPartitions(10))
            .setCacheMode(cm1)
            .setAtomicityMode(cam1)).withAllowAtomicOpsInTx();

        IgniteCache acc = srv0.createCache(accountCacheConfiguration(grp2, "acc")
            .setAffinity(new RendezvousAffinityFunction().setPartitions(10))
            .setCacheMode(cm2)
            .setAtomicityMode(cam2)).withAllowAtomicOpsInTx();

        try(Transaction tx = cam1 == TRANSACTIONAL || cam2 == TRANSACTIONAL ? srv0.transactions().txStart() : null) {
            for (int i = 0; i < keys; i++) {

                int pKey = i - (i % accsPerPerson);

                if (i % accsPerPerson == 0)
                    pers.put(pKey, new Person("pers-" + pKey));

                acc.put(new AffinityKey(i, pKey), new Account(pKey, "acc-" + i));
            }

            if (tx != null)
                tx.commit();
        }

        Ignite node = ignite(2);

        SqlFieldsQuery qry = new SqlFieldsQuery(
            "select p._key as p_key, p.name, a._key as a_key, a.personId, a.attr \n" +
            "from \"pers\".Person p inner join \"acc\".Account a \n" +
            "on (p._key = a.personId)");

        IgniteCache<Object, Object> cache = node.cache("acc");

        List<List<?>> res = cache.query(qry).getAll();

        assertEquals(keys, res.size());

        for (List<?> row : res)
            assertEquals(row.get(0), row.get(3));
    }

    /**
     * @param grpName Group name.
     * @param cacheName Cache name.
     * @return Person cache configuration.
     */
    private CacheConfiguration personCacheConfiguration(String grpName, String cacheName) {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person.class.getName());
        entity.addQueryField("name", String.class.getName(), null);

        return cacheConfiguration(grpName, cacheName, entity);
    }

    /**
     * @param grpName Group name.
     * @param cacheName Cache name.
     * @return Account cache configuration.
     */
    private CacheConfiguration accountCacheConfiguration(String grpName, String cacheName) {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(AffinityKey.class.getName());
        entity.setValueType(Account.class.getName());
        entity.addQueryField("personId", Integer.class.getName(), null);
        entity.addQueryField("attr", String.class.getName(), null);
        entity.setIndexes(F.asList(new QueryIndex("personId")));

        return cacheConfiguration(grpName, cacheName, entity);
    }

    /**
     * @param grpName Group name.
     * @param cacheName Cache name.
     * @param queryEntity Query entity.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String grpName, String cacheName, QueryEntity queryEntity) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setGroupName(grpName);
        ccfg.setName(cacheName);

        ccfg.setQueryEntities(F.asList(queryEntity));

        return ccfg;
    }

    /**
     *
     */
    private static class Person implements Serializable {
        /** */
        String name;

        /**
         * @param name Name.
         */
        public Person(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }

    /**
     *
     */
    private static class Account implements Serializable {
        /** */
        Integer personId;

        /** */
        String attr;

        /**
         * @param personId Person ID.
         * @param attr Attribute (some data).
         */
        public Account(Integer personId, String attr) {
            this.personId = personId;
            this.attr = attr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }
}
