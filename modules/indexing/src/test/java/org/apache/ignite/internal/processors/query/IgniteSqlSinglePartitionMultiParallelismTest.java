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

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.junit.Assert.assertEquals;

/**
 * IGNITE-14120 Test for correct results in case of query with single partition and cache with parallelism > 1
 * */
public class IgniteSqlSinglePartitionMultiParallelismTest {

    /**
     * Test timeout
     * */
    @Rule
    public Timeout globalTimeout = new Timeout((int)GridTestUtils.DFLT_TEST_TIMEOUT);

    /**
     * Check common case without partitions. Should be single result
     * */
    @Test
    public void assertSimpleCountQuery() throws Exception {
        run(client -> {
            List<List<?>> results = runQuery(client, "select count(*) from SC_NULL_TEST");

            Long res = (Long) results.get(0).get(0);

            assertEquals(1, results.size());
            assertEquals(Long.valueOf(999), res);
        });
    }

    /**
     * Check case with 1 partition. Partition segment must be calculated correctly
     * */
    @Test
    public void assertWhereCountFirstPartitionQuery() throws Exception {
        run(client -> {
            List<List<?>> results = runQuery(client, "select count(*) from SC_NULL_TEST where ID=1");

            Long res = (Long) results.get(0).get(0);

            assertEquals(1, results.size());
            assertEquals(Long.valueOf(1), res);
        });
    }

    /**
     * Check case with 1 partition. Partition segment must be calculated correctly
     * */
    @Test
    public void assertWhereCountAnotherPartitionQuery() throws Exception {
        run(client -> {
            List<List<?>> results = runQuery(client, "select count(*) from SC_NULL_TEST where ID=973");

            Long res = (Long) results.get(0).get(0);

            assertEquals(1, results.size());
            assertEquals(Long.valueOf(1), res);
        });
    }

    /**
     * Check case with 2 partition. Multiple partitions should not be affected
     * */
    @Test
    public void assertWhereCountMultiPartitionsQuery() throws Exception {
        run(client -> {
            List<List<?>> results = runQuery(client, "select count(*) from SC_NULL_TEST where ID=5 or ID=995");

            Long res = (Long) results.get(0).get(0);

            assertEquals(1, results.size());
            assertEquals(Long.valueOf(2), res);
        });
    }

    /**
     * */
    private void run(Consumer<IgniteClient> test) throws Exception {
        IgniteConfiguration srvCfg = Config.getServerConfiguration();

        srvCfg.setCacheConfiguration(
            new CacheConfiguration<Integer, Integer>()
                .setName("cache-template*")
                .setCacheMode(PARTITIONED)
                .setReadFromBackup(false)
                .setStatisticsEnabled(true)
                .setQueryParallelism(100)
                .setSqlIndexMaxInlineSize(64)
                .setBackups(0)
                .setPartitionLossPolicy(READ_WRITE_SAFE)
                .setAtomicityMode(TRANSACTIONAL)
        );

        try (
            Ignite ignored = Ignition.start(srvCfg);
            IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            createTable(client);

            test.accept(client);
        }
    }

    /**
     * */
    public void createTable(IgniteClient client) {
        runQuery(client,
            "CREATE TABLE IF NOT EXISTS SC_NULL_TEST ( id INT(11), val INT(11), PRIMARY KEY (ID) ) " +
                "WITH \"template=cache-template, CACHE_NAME=SC_NULL_TEST\""
        );

        for (int i = 1; i < 1000; i++)
            insertValue(client, i);
    }

    /**
     * */
    public void insertValue(IgniteClient client, int val) {
        runQuery(client, String.format("insert into SC_NULL_TEST VALUES(%d, %d)", val, val));
    }

    /**
     * */
    public List<List<?>> runQuery(IgniteClient client, String qry) {
        return client.query(
            new SqlFieldsQuery(qry)
        ).getAll();
    }

    /**
     * */
    private static ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setSendBufferSize(0)
            .setReceiveBufferSize(0);
    }
}
