/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for empty transaction while is then enlisted with real value.
 */
@RunWith(JUnit4.class)
public class MvccEmptyTransactionSelfTest extends GridCommonAbstractTest {
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
