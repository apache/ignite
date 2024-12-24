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

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class H2TransactionAwareQueriesEnabledTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);
        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /** */
    @Test
    public void testFailOnSqlQueryInsideTransaction() throws Exception {
        Ignite srv = startGrid(0);
        Ignite cli = startClientGrid(1);

        sql(srv, "CREATE TABLE T(ID BIGINT PRIMARY KEY, NAME VARCHAR) WITH \"atomicity=transactional\"");

        awaitPartitionMapExchange();

        for (Ignite node : new Ignite[]{srv, cli}) {
            for (TransactionConcurrency txConcurrenty : TransactionConcurrency.values()) {
                for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                    try (Transaction tx = node.transactions().txStart(txConcurrenty, txIsolation)) {
                        assertThrows(
                            null,
                            () -> sql(node, "SELECT * FROM T"),
                            IgniteSQLException.class,
                            "SQL aware queries are not supported by Indexing query engine"
                        );

                        tx.rollback();
                    }
                }
            }
        }

    }

    /** */
    public List<List<?>> sql(Ignite node, String sql) {
        return node.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)).getAll();
    }
}
