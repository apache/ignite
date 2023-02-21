/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.AbstractIncrementalSnapshotTest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;

/** */
public class IgniteIncrementalSnapshotRestoreWithIndexingTest extends AbstractIncrementalSnapshotTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testRestoredIndexes() throws Exception {
        grid(0).addCacheConfiguration(
            cacheConfiguration(CACHE)
                .setAffinity(new RendezvousAffinityFunction(false, 10)));

        sql("create table Account(id int primary key, balance int) WITH \"TEMPLATE=CACHE, VALUE_TYPE=Account, CACHE_NAME=SQL_CACHE\"");
        sql("create index on Account(balance);");

        Map<Integer, BinaryObject> expSnpData = new HashMap<>();

        loadData(expSnpData);

        grid(0).snapshot().createSnapshot(SNP).get(getTestTimeout());

        loadData(expSnpData);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        restartWithCleanPersistence(nodes(), F.asList(CACHE));

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData);
    }

    /** */
    @Override protected CacheConfiguration<Integer, Integer> cacheConfiguration(String name) {
        return super.cacheConfiguration(name)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(10));
    }

    /** */
    private void checkData(Map<Integer, BinaryObject> expData) {
        List<List<?>> actData = sql("select _KEY, _VAL from Account");

        assertEquals(expData.size(), actData.size());

        for (List<?> e: actData) {
            assertTrue("Missed: " + e, expData.containsKey((Integer)e.get(0)));
            assertEquals(e.toString(), expData.get((Integer)e.get(0)), e.get(1));
        }

        List<Cache.Entry<Integer, BinaryObject>> idxResAct = grid(0).cache("SQL_CACHE").withKeepBinary().query(
            new IndexQuery<Integer, BinaryObject>("Account")
                .setCriteria(gt("balance", 0))
        ).getAll();

        Map<Integer, BinaryObject> idxResExp = expData.entrySet().stream()
            .filter((e) -> (int)e.getValue().field("balance") > 0)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(idxResExp.size(), idxResAct.size());

        for (Cache.Entry<Integer, BinaryObject> e: idxResAct) {
            assertTrue("Missed: " + e, idxResExp.containsKey(e.getKey()));
            assertEquals(e.toString(), expData.get(e.getKey()), e.getValue());
        }
    }

    /** */
    private void loadData(Map<Integer, BinaryObject> data) {
        int bound = 200;

        String insertQry = "insert into Account(id, balance) values (?, ?)";
        String insertMultQry = "insert into Account(id, balance) values (?, ?), (?, ?)";
        String updateQry = "update Account set balance = ? where id = ?";
        String deleteQry = "delete from Account where id = ?";

        Random rnd = new Random();

        for (int i = 0; i < 100; i++) {
            Operation op = Operation.values()[rnd.nextInt(Operation.values().length)];

            switch (op) {
                case INSERT:
                    int putId = id(rnd, bound, data, true);
                    int putBalance = rnd.nextInt();

                    data.put(putId, account(putBalance));

                    sql(insertQry, putId, putBalance);

                    break;

                case INSERT_MULTIPLE:
                    int putId1 = id(rnd, bound, data, true);
                    int putBalance1 = rnd.nextInt();

                    data.put(putId1, account(putBalance1));

                    int putId2 = id(rnd, bound, data, true);
                    int putBalance2 = rnd.nextInt();

                    data.put(putId2, account(putBalance2));

                    sql(insertMultQry, putId1, putBalance1, putId2, putBalance2);

                    break;

                case UPDATE:
                    if (data.isEmpty())
                        break;

                    int updId = id(rnd, bound, data, false);
                    int updBalance = rnd.nextInt();

                    data.put(updId, account(updBalance));

                    sql(updateQry, updBalance, updId);

                    break;

                case DELETE:
                    if (data.isEmpty())
                        break;

                    int rmId = id(rnd, bound, data, false);

                    data.remove(rmId);

                    sql(deleteQry, rmId);

                    break;
            }
        }
    }

    /** */
    private List<List<?>> sql(String clause, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(clause).setArgs(args), true).getAll();
    }

    /** */
    private BinaryObject account(int balance) {
        return grid(0).binary().builder("Account")
            .setField("balance", balance)
            .build();
    }

    /** */
    private int id(Random rnd, int bound, Map<Integer, BinaryObject> data, boolean skipIfContains) {
        int id;

        do {
            id = rnd.nextInt(bound);
        }
        while (skipIfContains == data.containsKey(id));

        return id;
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }

    /** */
    private enum Operation {
        /** */
        INSERT,

        /** */
        INSERT_MULTIPLE,

        /** */
        UPDATE,

        /** */
        DELETE
    }
}
