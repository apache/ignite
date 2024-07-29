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

package org.apache.ignite.internal.processors.tx;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.tx.TransactionIsolationTest.executeSql;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_ALLOW_TX_AWARE_QUERIES, value = "true")
public class TransactionVisibilityTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    /** */
    @Test
    public void testVisibility() throws Exception {
        IgniteEx srv = startGrid();

        IgniteCache<Integer, Integer> cache = srv.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("TBL")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setTableName("TBL")
                .setKeyType(Integer.class.getName())
                .setValueType(Integer.class.getName()))));

        try (Transaction tx = srv.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 5_000, 10)) {
            cache.put(1, 2);

            assertEquals("Must see transaction related data", (Integer)2, cache.get(1));

            List<List<?>> sqlData = executeSql(srv, "SELECT COUNT(*) FROM TBL.TBL");

            assertEquals("Must see transaction related data", 1L, sqlData.get(0).get(0));

            tx.commit();
        }

        List<List<?>> sqlData = executeSql(srv, "SELECT COUNT(*) FROM TBL.TBL");

        assertEquals("Must see committed data", 1L, sqlData.get(0).get(0));
    }
}
