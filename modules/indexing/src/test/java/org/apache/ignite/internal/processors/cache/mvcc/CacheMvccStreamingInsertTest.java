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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Arrays.asList;

/**
 *
 */
@RunWith(JUnit4.class)
public class CacheMvccStreamingInsertTest extends CacheMvccAbstractTest {
    /** */
    private IgniteCache<Object, Object> sqlNexus;

    /** */
    private Connection conn;

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

        Properties props = new Properties();
        props.setProperty(IgniteJdbcDriver.PROP_STREAMING, "true");
        conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1", props);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStreamingInsertWithoutOverwrite() throws Exception {
        conn.createStatement().execute("SET STREAMING 1 BATCH_SIZE 2 ALLOW_OVERWRITE 0 " +
            " PER_NODE_BUFFER_SIZE 1000 FLUSH_FREQUENCY 100");
        sqlNexus.query(q("insert into person values(1, 'ivan')"));

        PreparedStatement batchStmt = conn.prepareStatement("insert into person values(?, ?)");
        batchStmt.setInt(1, 1);
        batchStmt.setString(2, "foo");
        batchStmt.addBatch();
        batchStmt.setInt(1, 2);
        batchStmt.setString(2, "bar");
        batchStmt.addBatch();
        TimeUnit.MILLISECONDS.sleep(500);

        List<List<?>> rows = sqlNexus.query(q("select * from person")).getAll();
        List<List<?>> exp = asList(
            asList(1, "ivan"),
            asList(2, "bar")
        );
        assertEquals(exp, rows);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateWithOverwrite() throws Exception {
        conn.createStatement().execute("SET STREAMING 1 BATCH_SIZE 2 ALLOW_OVERWRITE 1 " +
            " PER_NODE_BUFFER_SIZE 1000 FLUSH_FREQUENCY 100");
        sqlNexus.query(q("insert into person values(1, 'ivan')"));

        PreparedStatement batchStmt = conn.prepareStatement("insert into person values(?, ?)");
        batchStmt.setInt(1, 1);
        batchStmt.setString(2, "foo");
        batchStmt.addBatch();
        batchStmt.setInt(1, 2);
        batchStmt.setString(2, "bar");
        batchStmt.addBatch();
        TimeUnit.MILLISECONDS.sleep(500);

        List<List<?>> rows = sqlNexus.query(q("select * from person")).getAll();
        List<List<?>> exp = asList(
            asList(1, "foo"),
            asList(2, "bar")
        );
        assertEquals(exp, rows);
    }

    /** */
    private static SqlFieldsQuery q(String sql) {
        return new SqlFieldsQuery(sql);
    }
}
