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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.io.File;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 *
 */
public class CacheMvccBulkLoadTest extends CacheMvccAbstractTest {
    /** */
    private IgniteCache<Object, Object> sqlNexus;

    /** */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Ignite ignite = startGrid(0);
        sqlNexus = ignite.getOrCreateCache(new CacheConfiguration<>("sqlNexus").setSqlSchema("PUBLIC"));
        sqlNexus.query(q("" +
            "create table person(" +
            "  id int not null primary key," +
            "  name varchar not null" +
            ") with \"atomicity=transactional_snapshot\""
        ));
        stmt = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1").createStatement();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCopyStoresData() throws Exception {
        String csvFilePath = new File(getClass().getResource("mvcc_person.csv").toURI()).getAbsolutePath();
        stmt.executeUpdate("copy from '" + csvFilePath + "' into person (id, name) format csv");

        List<List<?>> rows = sqlNexus.query(q("select * from person")).getAll();

        List<List<? extends Serializable>> exp = asList(
            asList(1, "John"),
            asList(2, "Jack")
        );
        assertEquals(exp, rows);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCopyDoesNotOverwrite() throws Exception {
        sqlNexus.query(q("insert into person values(1, 'Old')"));
        String csvFilePath = new File(getClass().getResource("mvcc_person.csv").toURI()).getAbsolutePath();
        stmt.executeUpdate("copy from '" + csvFilePath + "' into person (id, name) format csv");

        List<List<?>> rows = sqlNexus.query(q("select * from person")).getAll();

        List<List<? extends Serializable>> exp = asList(
            asList(1, "Old"),
            asList(2, "Jack")
        );
        assertEquals(exp, rows);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCopyLeavesPartialResultsInCaseOfFailure() throws Exception {
        String csvFilePath = new File(getClass().getResource("mvcc_person_broken.csv").toURI()).getAbsolutePath();
        try {
            stmt.executeUpdate("copy from '" + csvFilePath + "' into person (id, name) format csv");
            fail();
        }
        catch (SQLException ignored) {
            // assert exception is thrown
        }

        List<List<?>> rows = sqlNexus.query(q("select * from person")).getAll();

        List<List<? extends Serializable>> exp = singletonList(
            asList(1, "John")
        );
        assertEquals(exp, rows);
    }

    /** */
    private static SqlFieldsQuery q(String sql) {
        return new SqlFieldsQuery(sql);
    }
}
