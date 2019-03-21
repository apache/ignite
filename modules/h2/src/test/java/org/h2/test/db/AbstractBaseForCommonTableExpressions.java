/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.h2.test.TestDb;

/**
 * Base class for common table expression tests
 */
public abstract class AbstractBaseForCommonTableExpressions extends TestDb {

    /**
     * Test a query.
     *
     * @param maxRetries the number of times the query is run
     * @param expectedRowData the expected result data
     * @param expectedColumnNames the expected columns of the result
     * @param expectedNumberOfRows the expected number of rows
     * @param setupSQL the SQL statement used for setup
     * @param withQuery the query
     * @param closeAndReopenDatabaseConnectionOnIteration whether the connection
     *            should be re-opened each time
     * @param expectedColumnTypes the expected datatypes of the result
     * @param anyOrder whether any order of rows should be allowed.
     *                 If {@code true}, this method may sort expectedRowData.
     */
    void testRepeatedQueryWithSetup(int maxRetries, String[] expectedRowData, String[] expectedColumnNames,
            int expectedNumberOfRows, String setupSQL, String withQuery,
            int closeAndReopenDatabaseConnectionOnIteration, String[] expectedColumnTypes,
            boolean anyOrder) throws SQLException {

        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        if (anyOrder) {
            Arrays.sort(expectedRowData);
        }
        ArrayList<String> rowData = new ArrayList<>();
        StringBuilder buf = new StringBuilder();

        for (int queryRunTries = 1; queryRunTries <= maxRetries; queryRunTries++) {

            Statement stat = conn.createStatement();
            stat.execute(setupSQL);
            stat.close();

            // close and re-open connection for one iteration to make sure the query work
            // between connections
            if (queryRunTries == closeAndReopenDatabaseConnectionOnIteration) {
                conn.close();

                conn = getConnection("commonTableExpressionQueries");
            }
            prep = conn.prepareStatement(withQuery);

            rs = prep.executeQuery();
            for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++) {

                assertNotNull(rs.getMetaData().getColumnLabel(columnIndex));
                assertEquals(expectedColumnNames[columnIndex - 1], rs.getMetaData().getColumnLabel(columnIndex));
                assertEquals(
                        "wrong type of column " + rs.getMetaData().getColumnLabel(columnIndex) + " on iteration #"
                                + queryRunTries,
                        expectedColumnTypes[columnIndex - 1], rs.getMetaData().getColumnTypeName(columnIndex));
            }

            rowData.clear();
            while (rs.next()) {
                buf.setLength(0);
                for (int columnIndex = 1; columnIndex <= rs.getMetaData().getColumnCount(); columnIndex++) {
                    buf.append('|').append(rs.getString(columnIndex));
                }
                rowData.add(buf.toString());
            }
            if (anyOrder) {
                Collections.sort(rowData);
            }
            assertEquals(expectedRowData, rowData.toArray(new String[0]));

            rs.close();
            prep.close();
        }

        conn.close();
        deleteDb("commonTableExpressionQueries");

    }

}
