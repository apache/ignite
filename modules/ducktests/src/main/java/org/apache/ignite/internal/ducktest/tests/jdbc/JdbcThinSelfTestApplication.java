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

package org.apache.ignite.internal.ducktest.tests.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Simple application working via the Thin JDBC driver.
 */
public class JdbcThinSelfTestApplication extends IgniteAwareApplication {
    /** */
    private static final BigDecimal DECIMAL_VALUE1 = BigDecimal.valueOf(762, 2);

    /** */
    private static final BigDecimal DECIMAL_VALUE2 = BigDecimal.valueOf(545, 2);

    /** */
    private static final String VARCHAR_VALUE = "ak";

    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws SQLException {
        markInitialized();

        try (Connection conn = thinJdbcDataSource.getConnection()) {
            createTable(conn);

            insertRow(conn);

            selectRow(conn);

            updateRow(conn);

            deleteRow(conn);

            dropTable(conn);

            markFinished();
        }
    }

    /** */
    private void createTable(Connection conn) throws SQLException {
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS table(id INT, strVal VARCHAR, decVal DECIMAL, PRIMARY KEY(id)) ");
    }

    /** */
    private void insertRow(Connection conn) throws SQLException {
        PreparedStatement insertStatement = conn.prepareStatement("INSERT INTO table(id, strVal, decVal) VALUES(?, ?, ?)");

        insertStatement.setInt(1, 1);
        insertStatement.setString(2, VARCHAR_VALUE);
        insertStatement.setBigDecimal(3, DECIMAL_VALUE1);

        insertStatement.execute();
    }

    /** */
    private void selectRow(Connection conn) throws SQLException {
        PreparedStatement selectStatement = conn.prepareStatement("SELECT * FROM table WHERE id = ?");

        selectStatement.setInt(1, 1);

        ResultSet resultSet = selectStatement.executeQuery();

        if (!resultSet.next())
            markBroken(new RuntimeException("ResultSet is empty."));

        int resultId = resultSet.getInt("id");
        String resultStr = resultSet.getString("strVal");
        BigDecimal resultDecimal = resultSet.getBigDecimal("decVal");

        if (1 != resultId || !VARCHAR_VALUE.equals(resultStr) || DECIMAL_VALUE1.compareTo(resultDecimal) != 0)
            throw new RuntimeException("Wrong row selected: " +
                    "expected [id=1, strVal=" + VARCHAR_VALUE + ", decVal=" + DECIMAL_VALUE1 + "], " +
                    "actual [id=" + resultId + ", strVal=" + resultStr + ", decVal=" + resultDecimal + "].");
    }

    /** */
    private void updateRow(Connection conn) throws SQLException {
        PreparedStatement selectStatement = conn.prepareStatement("UPDATE table SET decVal = ? WHERE id = ?");

        selectStatement.setBigDecimal(1, DECIMAL_VALUE2);
        selectStatement.setInt(2, 1);

        if (1 != selectStatement.executeUpdate())
            throw new RuntimeException("Row wasn't updated.");
    }

    /** */
    private void deleteRow(Connection conn) throws SQLException {
        PreparedStatement selectStatement = conn.prepareStatement("DELETE FROM table WHERE id = ?");

        selectStatement.setInt(1, 1);

        if (1 != selectStatement.executeUpdate())
            throw new RuntimeException("Row wasn't deleted.");
    }

    /** */
    private void dropTable(Connection conn) throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS table");
    }
}
