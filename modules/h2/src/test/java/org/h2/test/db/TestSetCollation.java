/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import java.util.List;
import org.h2.jdbc.JdbcSQLException;
import org.h2.test.TestBase;

public class TestSetCollation extends TestBase {
    private static final String[] TEST_STRINGS = new String[]{"A", "\u00c4", "AA", "B", "$", "1A", null};

    private static final String DB_NAME = "collator";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testDefaultCollator();
        testCp500Collator();
        testDeCollator();
        testUrlParameter();
        testReopenDatabase();
        testReopenDatabaseWithUrlParameter();
        testReopenDatabaseWithDifferentCollationInUrl();
        testReopenDatabaseWithSameCollationInUrl();
    }


    private void testDefaultCollator() throws Exception {
        assertEquals(Arrays.asList(null, "$", "1A", "A", "AA", "B", "\u00c4"), orderedWithCollator(null));
    }

    private void testDeCollator() throws Exception {
        assertEquals(Arrays.asList(null, "$", "1A", "A", "\u00c4", "AA", "B"), orderedWithCollator("DE"));
        assertEquals(Arrays.asList(null, "$", "1A", "A", "\u00c4", "AA", "B"), orderedWithCollator("DEFAULT_DE"));
    }

    private void testCp500Collator() throws Exception {
        // IBM z/OS codepage
        assertEquals(Arrays.asList(null, "A", "AA", "B", "1A", "$", "\u00c4"),
                orderedWithCollator("CHARSET_CP500"));
    }

    private void testUrlParameter() throws Exception {
        // Specifying the collator in the JDBC Url should have the same effect
        // as setting it with a set statement
        config.collation = "CHARSET_CP500";
        try {
            assertEquals(Arrays.asList(null, "A", "AA", "B", "1A", "$", "\u00c4"), orderedWithCollator(null));
        } finally {
            config.collation = null;
        }
    }

    private void testReopenDatabase() throws Exception {
        if (config.memory) {
            return;
        }

        orderedWithCollator("DE");

        try (Connection con = getConnection(DB_NAME)) {
            insertValues(con, new String[]{"A", "\u00c4"}, 100);

            assertEquals(Arrays.asList(null, "$", "1A", "A", "A", "\u00c4", "\u00c4", "AA", "B"),
                    loadTableValues(con));
        }
    }

    private void testReopenDatabaseWithUrlParameter() throws Exception {
        if (config.memory) {
            return;
        }

        config.collation = "DE";
        try {
            orderedWithCollator(null);
        } finally {
            config.collation = null;
        }

        // reopen the database without specifying a collation in the url.
        // This should keep the initial collation.
        try (Connection con = getConnection(DB_NAME)) {
            insertValues(con, new String[]{"A", "\u00c4"}, 100);

            assertEquals(Arrays.asList(null, "$", "1A", "A", "A", "\u00c4", "\u00c4", "AA", "B"),
                    loadTableValues(con));
        }

    }

    private void testReopenDatabaseWithDifferentCollationInUrl() throws Exception {
        if (config.memory) {
            return;
        }
        config.collation = "DE";
        try {
            orderedWithCollator(null);
        } finally {
            config.collation = null;
        }

        config.collation = "CHARSET_CP500";
        try {
            getConnection(DB_NAME);
            fail();
        } catch (JdbcSQLException e) {
            // expected
        } finally {
            config.collation = null;
        }
    }

    private void testReopenDatabaseWithSameCollationInUrl() throws Exception {
        if (config.memory) {
            return;
        }
        config.collation = "DE";
        try {
            orderedWithCollator(null);
        } finally {
            config.collation = null;
        }

        config.collation = "DE";
        try (Connection con = getConnection(DB_NAME)) {
            insertValues(con, new String[]{"A", "\u00c4"}, 100);

            assertEquals(Arrays.asList(null, "$", "1A", "A", "A", "\u00c4", "\u00c4", "AA", "B"),
                    loadTableValues(con));
        } finally {
            config.collation = null;
        }
    }


    private List<String> orderedWithCollator(String collator) throws SQLException {
        deleteDb(DB_NAME);
        try (Connection con = getConnection(DB_NAME); Statement statement = con.createStatement()) {
            if (collator != null) {
                statement.execute("SET COLLATION " + collator);
            }
            statement.execute("CREATE TABLE charsettable(id INT PRIMARY KEY, testvalue VARCHAR(50))");

            insertValues(con, TEST_STRINGS, 1);

            return loadTableValues(con);
        }
    }

    private static void insertValues(Connection con, String[] values, int startId) throws SQLException {
        PreparedStatement ps = con.prepareStatement("INSERT INTO charsettable VALUES (?, ?)");
        int id = startId;
        for (String value : values) {
            ps.setInt(1, id++);
            ps.setString(2, value);
            ps.execute();
        }
        ps.close();
    }

    private static List<String> loadTableValues(Connection con) throws SQLException {
        List<String> results = new ArrayList<>();
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("select testvalue from charsettable order by testvalue");
        while (resultSet.next()) {
            results.add(resultSet.getString(1));
        }
        statement.close();
        return results;
    }

}
