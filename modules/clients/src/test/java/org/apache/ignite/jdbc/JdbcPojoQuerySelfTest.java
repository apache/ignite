/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.jdbc;

import java.sql.ResultSet;
import org.junit.Test;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Test for Jdbc driver query without class on client
 */
public class JdbcPojoQuerySelfTest extends AbstractJdbcPojoQuerySelfTest {
    /** URL. */
    private static final String URL = CFG_URL_PREFIX + "cache=default@modules/clients/src/test/config/jdbc-bin-config.xml";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcQueryTask2() throws Exception {
        stmt.execute("select * from JdbcTestObject");

        ResultSet rs = stmt.getResultSet();

        assertResultSet(rs);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJdbcQueryTask1() throws Exception {
        ResultSet rs = stmt.executeQuery("select * from JdbcTestObject");

        assertResultSet(rs);

    }

    /** {@inheritDoc} */
    @Override protected String getURL() {
        return URL;
    }
}
