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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 * Test for empty transaction while is then enlisted with real value.
 */
public class MvccEmptyTransactionSelfTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testEmptyTransaction() throws Exception {
        Ignition.start(config("srv", false));

        Ignite cli = Ignition.start(config("cli", true));

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10801")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR) " +
                    "WITH \"atomicity=TRANSACTIONAL_SNAPSHOT, cache_name=PERSON_CACHE\"");
            }
        }

        IgniteCache cache = cli.cache("PERSON_CACHE");

        try (Transaction tx = cli.transactions().txStart()) {
            // This will cause empty near TX to be created and then rolled back.
            cache.query(new SqlFieldsQuery("UPDATE person SET name=?").setArgs("Petr")).getAll();

            // One more time.
            cache.query(new SqlFieldsQuery("UPDATE person SET name=?").setArgs("Petr")).getAll();

            // Normal transaction is created, and several updates are performed.
            cache.query(new SqlFieldsQuery("INSERT INTO person VALUES (?, ?)").setArgs(1, "Ivan")).getAll();
            cache.query(new SqlFieldsQuery("UPDATE person SET name=?").setArgs("Sergey")).getAll();

            // Another update with empty response.
            cache.query(new SqlFieldsQuery("UPDATE person SET name=? WHERE name=?").setArgs("Vasiliy", "Ivan")).getAll();

            // One more normal update.
            cache.query(new SqlFieldsQuery("UPDATE person SET name=?").setArgs("Vsevolod")).getAll();

            tx.commit();
        }

        List<List<Object>> res = cache.query(new SqlFieldsQuery("SELECT name FROM person")).getAll();

        assert res.size() == 1;
        assert res.get(0).size() == 1;

        assertEquals("Vsevolod", (String)res.get(0).get(0));
    }

    /**
     * Create config.
     *
     * @param name Name.
     * @param client Client flag.
     * @return Config.
     */
    private static IgniteConfiguration config(String name, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);
        cfg.setClientMode(client);

        return cfg;
    }
}
