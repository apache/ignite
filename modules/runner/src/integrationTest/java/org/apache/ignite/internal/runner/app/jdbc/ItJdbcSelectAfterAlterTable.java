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

package org.apache.ignite.internal.runner.app.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Base class for complex SQL tests based on JDBC driver.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcSelectAfterAlterTable extends AbstractJdbcSelfTest {
    /** {@inheritDoc} */
    @BeforeEach
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stmt.executeUpdate("CREATE TABLE PUBLIC.PERSON (ID BIGINT, NAME VARCHAR, CITY_ID BIGINT, PRIMARY KEY (ID, CITY_ID))");
        stmt.executeUpdate("INSERT INTO PUBLIC.PERSON (ID, NAME, CITY_ID) values (1, 'name_1', 11)");
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        clusterNodes.get(0).tables().dropTableAsync("PUBLIC.PERSON").get();
    }

    /**
     * Alter table test.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSelectAfterAlterTableSingleNode() throws Exception {
        stmt.executeUpdate("alter table PUBLIC.PERSON add AGE int");

        checkNewColumn(stmt);
    }

    /**
     * New column check.
     *
     * @param stmt Statement to check new column.
     *
     * @throws SQLException If failed.
     */
    public void checkNewColumn(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("select * from PUBLIC.PERSON");

        ResultSetMetaData meta = rs.getMetaData();

        assertEquals(4, meta.getColumnCount());

        boolean newColExists = false;

        for (int i = 1; i <= meta.getColumnCount(); ++i) {
            if ("AGE".equalsIgnoreCase(meta.getColumnName(i))) {
                newColExists = true;

                break;
            }
        }

        assertTrue(newColExists);
    }
}
