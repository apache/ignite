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

package org.apache.ignite.jdbc;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test for Jdbc driver query without class on client
 */
public abstract class AbstractJdbcPojoQuerySelfTest extends GridCommonAbstractTest {
    /** TestObject class name. */
    protected static final String TEST_OBJECT = "org.apache.ignite.internal.JdbcTestObject";

    /** TestObject class name. */
    protected static final String TEST_OBJECT_2 = "org.apache.ignite.internal.JdbcTestObject2";

    /** Statement. */
    protected Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setAtomicityMode(TRANSACTIONAL);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        QueryEntity queryEntity = new QueryEntity();
        queryEntity.setKeyType("java.lang.String");
        queryEntity.setValueType("org.apache.ignite.internal.JdbcTestObject");
        queryEntity.addQueryField("id", "java.lang.Integer", null);
        queryEntity.addQueryField("testObject", "org.apache.ignite.internal.JdbcTestObject2", null);
        queryEntity.setIndexes(Collections.singletonList(new QueryIndex("id")));

        cache.setQueryEntities(Collections.singletonList(queryEntity));

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite ignite = startGrid(0);

        BinaryObjectBuilder builder = ignite.binary().builder(TEST_OBJECT);
        BinaryObjectBuilder builder2 = ignite.binary().builder(TEST_OBJECT_2);

        builder2.setField("id", 1);
        builder2.setField("boolVal", true);

        BinaryObject testObject = builder2.build();

        builder.setField("id", 1);
        builder.setField("testObject", testObject);

        BinaryObject binObj = builder.build();

        IgniteCache<String, BinaryObject> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        cache.put("0", binObj);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stmt = DriverManager.getConnection(getURL()).createStatement();

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null) {
            stmt.getConnection().close();
            stmt.close();

            assertTrue(stmt.isClosed());
        }
    }

    /**
     * @param rs Result set.
     * @throws Exception In case of error.
     */
    protected void assertResultSet(ResultSet rs) throws Exception {
        assertNotNull(rs);

        int cnt = 0;

        while (rs.next()) {
            assertNotNull(rs.getString("id"));
            assertNotNull(rs.getString("testObject"));

            assertTrue(rs.getObject("testObject").toString().contains("id=1"));
            assertTrue(rs.getObject("testObject").toString().contains("boolVal=true"));

            cnt++;
        }

        assertEquals(1, cnt);
    }

    /**
     * @return URL.
     */
    protected abstract String getURL();
}
