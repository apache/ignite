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
import org.h2.api.Trigger;
import org.h2.test.TestBase;

/**
 * Test merge using syntax.
 */
public class TestMergeUsing extends TestBase implements Trigger {

    private static final String GATHER_ORDERED_RESULTS_SQL = "SELECT ID, NAME FROM PARENT ORDER BY ID ASC";
    private static int triggerTestingUpdateCount;

    private String triggerName;

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

        // Simple ID,NAME inserts, target table with PK initially empty
        testMergeUsing(
                "CREATE TABLE PARENT(ID INT, NAME VARCHAR, PRIMARY KEY(ID) );",
                "MERGE INTO PARENT AS P USING (SELECT X AS ID, 'Marcy'||X AS NAME " +
                "FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID AND 1=1 AND S.ID = P.ID) " +
                "WHEN MATCHED THEN " +
                "UPDATE SET P.NAME = S.NAME WHERE 2 = 2 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)", 2);
        // Simple NAME updates, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S " +
                "ON (P.ID = S.ID AND 1=1 AND S.ID = P.ID) " +
                "WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE 1 = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(1,2)",
                2);
        // No NAME updates, WHERE clause is always false, insert clause missing
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID) " +
                "WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE 1 = 2",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)", 0);
        // No NAME updates, no WHERE clause, insert clause missing
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID) " +
                "WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(1,2)",
                2);
        // Two delete updates done, no WHERE clause, insert clause missing
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) ) AS S ON (P.ID = S.ID) " +
                "WHEN MATCHED THEN DELETE",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) WHERE 1=0",
                2);
        // One insert, one update one delete happens, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) ) AS S ON (P.ID = S.ID) " +
                "WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE P.ID = 2 " +
                "DELETE WHERE P.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3);
        // No updates happen: No insert defined, no update or delete happens due
        // to ON condition failing always, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) ) AS S ON (P.ID = S.ID AND 1=0) " +
                "WHEN MATCHED THEN " +
                "UPDATE SET P.NAME = S.NAME||S.ID WHERE P.ID = 2 DELETE WHERE P.ID = 1",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)", 0);
        // One insert, one update one delete happens, target table missing PK
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );" +
                "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT AS P USING SOURCE AS S ON (P.ID = S.ID) WHEN MATCHED THEN " +
                "UPDATE SET P.NAME = S.NAME||S.ID WHERE P.ID = 2 DELETE WHERE P.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3);
        // One insert, one update one delete happens, target table missing PK,
        // no source alias
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );" +
                "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT AS P USING SOURCE ON (P.ID = SOURCE.ID) WHEN MATCHED THEN " +
                "UPDATE SET P.NAME = SOURCE.NAME||SOURCE.ID WHERE P.ID = 2 DELETE WHERE P.ID = 1 " +
                "WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3);
        // One insert, one update one delete happens, target table missing PK,
        // no source or target alias
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2) );" +
                "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN " +
                "UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 " +
                "DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3);

        // Only insert clause, no update or delete clauses
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,1) );" +
                "DELETE FROM PARENT;",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) ) AS S ON (P.ID = S.ID) " +
                "WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)", 3);
        // no insert, no update, no delete clauses - essentially a no-op
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,1) );"
                        + "DELETE FROM PARENT;",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) ) AS S ON (P.ID = S.ID)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) WHERE X<0",
                0,
                "At least UPDATE, DELETE or INSERT embedded statement must be supplied.");
        // Two updates to same row - update and delete together - emptying the
        // parent table
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,1) )",
                "MERGE INTO PARENT AS P USING (" +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) ) AS S ON (P.ID = S.ID) " +
                "WHEN MATCHED THEN " +
                "UPDATE SET P.NAME = P.NAME||S.ID WHERE P.ID = 1 DELETE WHERE P.ID = 1 AND P.NAME = 'Marcy11'",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,1) WHERE X<0",
                2);
        // Duplicate source keys but different ROWID update - so no error
        // SQL standard says duplicate or repeated updates of same row in same
        // statement should cause errors - but because first row is updated,
        // deleted (on source row 1) then inserted (on source row 2)
        // it's considered different - with respect to to ROWID - so no error
        // One insert, one update one delete happens (on same row) , target
        // table missing PK, no source or target alias
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,1) );" +
                "CREATE TABLE SOURCE AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN " +
                "UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 " +
                "DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT 1 AS ID, 'Marcy'||X||X UNION ALL SELECT 1 AS ID, 'Marcy2'",
                2);

        // Multiple update on same row: SQL standard says duplicate or repeated
        // updates in same statement should cause errors -but because first row
        // is updated, delete then insert it's considered different
        // One insert, one update one delete happens (on same row, which is
        // okay), then another update (which is illegal)target table missing PK,
        // no source or target alias
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,1) );"
                        + "CREATE TABLE SOURCE AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN " +
                "UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 " +
                "DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT 1 AS ID, 'Marcy'||X||X UNION ALL SELECT 1 AS ID, 'Marcy2'",
                3,
                "Unique index or primary key violation: \"Merge using " +
                "ON column expression, duplicate _ROWID_ target record " +
                "already updated, deleted or inserted:_ROWID_=2:in:PUBLIC.PARENT:conflicting source row number:2");
        // Duplicate key updated 3 rows at once, only 1 expected
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) );"
                        + "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN " +
                "UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 " +
                "DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3, "Duplicate key updated 3 rows at once, only 1 expected");
        // Missing target columns in ON expression
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) );"
                        + "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (1 = SOURCE.ID) WHEN MATCHED THEN " +
                "UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 " +
                "DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3, "No references to target columns found in ON clause");
        // Missing source columns in ON expression
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3) );"
                        + "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = 1) WHEN MATCHED THEN " +
                "UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 " +
                "DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (SOURCE.ID, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X||X AS NAME FROM SYSTEM_RANGE(2,2) UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(3,3)",
                3, "No references to source columns found in ON clause");
        // Insert does not insert correct values with respect to ON condition
        // (inserts ID value above 100, instead)
        testMergeUsingException(
                "CREATE TABLE PARENT AS (SELECT 1 AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(4,4) );"
                        + "CREATE TABLE SOURCE AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,3)  );",
                "MERGE INTO PARENT USING SOURCE ON (PARENT.ID = SOURCE.ID) WHEN MATCHED THEN " +
                "UPDATE SET PARENT.NAME = SOURCE.NAME||SOURCE.ID WHERE PARENT.ID = 2 " +
                "DELETE WHERE PARENT.ID = 1 WHEN NOT MATCHED THEN " +
                "INSERT (ID, NAME) VALUES (SOURCE.ID+100, SOURCE.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(4,4)", 1,
                "Expected to find key after row inserted, but none found. Insert does not match ON condition.");
        // One insert, one update one delete happens, target table missing PK,
        // triggers update all NAME fields
        triggerTestingUpdateCount = 0;
        testMergeUsing(
                "CREATE TABLE PARENT AS (SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,2));"
                        + getCreateTriggerSQL(),
                "MERGE INTO PARENT AS P USING " +
                "(SELECT X AS ID, 'Marcy'||X AS NAME FROM SYSTEM_RANGE(1,4) ) AS S ON (P.ID = S.ID) " +
                "WHEN MATCHED THEN UPDATE SET P.NAME = S.NAME||S.ID WHERE P.ID = 2 " +
                "DELETE WHERE P.ID = 1 WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (S.ID, S.NAME)",
                GATHER_ORDERED_RESULTS_SQL,
                "SELECT 2 AS ID, 'Marcy22-updated2' AS NAME UNION ALL " +
                "SELECT X AS ID, 'Marcy'||X||'-inserted'||X AS NAME FROM SYSTEM_RANGE(3,4)",
                4);
    }

    /**
     * Run a test case of the merge using syntax
     *
     * @param setupSQL - one or more SQL statements to setup the case
     * @param statementUnderTest - the merge statement being tested
     * @param gatherResultsSQL - a select which gathers the results of the merge
     *            from the target table
     * @param expectedResultsSQL - a select which returns the expected results
     *            in the target table
     * @param expectedRowUpdateCount - how many updates should be expected from
     *            the merge using
     */
    private void testMergeUsing(String setupSQL, String statementUnderTest,
            String gatherResultsSQL, String expectedResultsSQL,
            int expectedRowUpdateCount) throws Exception {
        deleteDb("mergeUsingQueries");

        try (Connection conn = getConnection("mergeUsingQueries")) {
            Statement stat = conn.createStatement();
            stat.execute(setupSQL);

            PreparedStatement prep = conn.prepareStatement(statementUnderTest);
            int rowCountUpdate = prep.executeUpdate();

            // compare actual results from SQL result set with expected results
            // - by diffing (aka set MINUS operation)
            ResultSet rs = stat.executeQuery("( " + gatherResultsSQL + " ) MINUS ( "
                    + expectedResultsSQL + " )");

            int rowCount = 0;
            StringBuilder diffBuffer = new StringBuilder("");
            while (rs.next()) {
                rowCount++;
                diffBuffer.append("|");
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    diffBuffer.append(rs.getObject(i));
                    diffBuffer.append("|\n");
                }
            }
            assertEquals("Differences between expected and actual output found:"
                    + diffBuffer, 0, rowCount);
            assertEquals("Expected update counts differ",
                    expectedRowUpdateCount, rowCountUpdate);
        } finally {
            deleteDb("mergeUsingQueries");
        }
    }

    /**
     * Run a test case of the merge using syntax
     *
     * @param setupSQL - one or more SQL statements to setup the case
     * @param statementUnderTest - the merge statement being tested
     * @param gatherResultsSQL - a select which gathers the results of the merge
     *            from the target table
     * @param expectedResultsSQL - a select which returns the expected results
     *            in the target table
     * @param expectedRowUpdateCount - how many updates should be expected from
     *            the merge using
     * @param exceptionMessage - the exception message expected
     */
    private void testMergeUsingException(String setupSQL,
            String statementUnderTest, String gatherResultsSQL,
            String expectedResultsSQL, int expectedRowUpdateCount,
            String exceptionMessage) throws Exception {
        try {
            testMergeUsing(setupSQL, statementUnderTest, gatherResultsSQL,
                    expectedResultsSQL, expectedRowUpdateCount);
        } catch (RuntimeException | org.h2.jdbc.JdbcSQLException e) {
            if (!e.getMessage().contains(exceptionMessage)) {
                e.printStackTrace();
            }
            assertContains(e.getMessage(), exceptionMessage);
            return;
        }
        fail("Failed to see exception with message:" + exceptionMessage);
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow)
            throws SQLException {

        if (conn == null) {
            throw new AssertionError("connection is null");
        }
        if (triggerName.startsWith("INS_BEFORE")) {
            newRow[1] = newRow[1] + "-inserted" + (++triggerTestingUpdateCount);
        } else if (triggerName.startsWith("UPD_BEFORE")) {
            newRow[1] = newRow[1] + "-updated" + (++triggerTestingUpdateCount);
        } else if (triggerName.startsWith("DEL_BEFORE")) {
            oldRow[1] = oldRow[1] + "-deleted" + (++triggerTestingUpdateCount);
        }
    }

    @Override
    public void close() {
        // ignore
    }

    @Override
    public void remove() {
        // ignore
    }

    @Override
    public void init(Connection conn, String schemaName, String trigger,
            String tableName, boolean before, int type) {
        this.triggerName = trigger;
        if (!"PARENT".equals(tableName)) {
            throw new AssertionError("supposed to be PARENT");
        }
        if ((trigger.endsWith("AFTER") && before)
                || (trigger.endsWith("BEFORE") && !before)) {
            throw new AssertionError(
                    "triggerName: " + trigger + " before:" + before);
        }
        if ((trigger.startsWith("UPD") && type != UPDATE)
                || (trigger.startsWith("INS") && type != INSERT)
                || (trigger.startsWith("DEL") && type != DELETE)) {
            throw new AssertionError(
                    "triggerName: " + trigger + " type:" + type);
        }
    }

    private String getCreateTriggerSQL() {
        StringBuilder buf = new StringBuilder();
        buf.append("CREATE TRIGGER INS_BEFORE " + "BEFORE INSERT ON PARENT "
                + "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\";");
        buf.append("CREATE TRIGGER UPD_BEFORE " + "BEFORE UPDATE ON PARENT "
                + "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\";");
        buf.append("CREATE TRIGGER DEL_BEFORE " + "BEFORE DELETE ON PARENT "
                + "FOR EACH ROW NOWAIT CALL \"" + getClass().getName() + "\";");
        return buf.toString();
    }

}
