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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Created by apaschenko on 29.08.17.
 */
public abstract class JdbcErrorsAbstractSelfTest extends GridCommonAbstractTest {
    protected static final String CFG_PATH = "modules/clients/src/test/config/jdbc-config.xml";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration(getTestIgniteInstanceName(0))
            .setCacheConfiguration(new CacheConfiguration("test")
                .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Integer.class)))));

        //startGrid(loadConfiguration(CFG_PATH).setIgniteInstanceName(getTestIgniteInstanceName(0)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    public void testTableErrors() {

    }

    public void testIndexErrors() {

    }

    public void testDmlErrors() throws SQLException {
        checkErrorState("INSERT INTO \"test\".INTEGER(_key, _val) values(1, null)", "22004");
    }

    protected abstract Connection getConnection() throws SQLException;

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void checkErrorState(String sql, String expState) throws SQLException {
        try (Connection conn = getConnection()) {
            try (final PreparedStatement stmt = conn.prepareStatement(sql)) {
                SQLException ex = (SQLException)GridTestUtils.assertThrows(null, new IgniteCallable<Void>() {
                    @Override public Void call() throws Exception {
                        stmt.execute();

                        fail();

                        return null;
                    }
                }, SQLException.class, null);

                assertEquals(expState, ex.getSQLState());
            }
        }
    }
}
