/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Test the impact of DROP VIEW statements on dependent views.
 */
public class TestViewDropView extends TestBase {

    private Connection conn;
    private Statement stat;

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
        deleteDb(getTestName());
        conn = getConnection(getTestName());
        stat = conn.createStatement();

        testDropViewDefaultBehaviour();
        testDropViewRestrict();
        testDropViewCascade();
        testCreateForceView();
        testCreateOrReplaceView();
        testCreateOrReplaceViewWithNowInvalidDependentViews();
        testCreateOrReplaceForceViewWithNowInvalidDependentViews();

        conn.close();
        deleteDb(getTestName());
    }

    private void testCreateForceView() throws SQLException {
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).
                execute("create view test_view as select * from test");
        stat.execute("create force view test_view as select * from test");
        stat.execute("create table test(id int)");
        stat.execute("alter view test_view recompile");
        stat.execute("select * from test_view");
        stat.execute("drop table test_view, test cascade");
        stat.execute("create force view test_view as select * from test where 1=0");
        stat.execute("create table test(id int)");
        stat.execute("alter view test_view recompile");
        stat.execute("select * from test_view");
        stat.execute("drop table test_view, test cascade");
    }

    private void testDropViewDefaultBehaviour() throws SQLException {
        createTestData();
        ResultSet rs = stat.executeQuery("select value " +
                "from information_schema.settings where name = 'DROP_RESTRICT'");
        rs.next();
        boolean dropRestrict = rs.getBoolean(1);
        if (dropRestrict) {
            // should fail because have dependencies
            assertThrows(ErrorCode.CANNOT_DROP_2, stat).
                execute("drop view v1");
        } else {
            stat.execute("drop view v1");
            checkViewRemainsValid();
        }
    }

    private void testDropViewRestrict() throws SQLException {
        createTestData();
        // should fail because have dependencies
        assertThrows(ErrorCode.CANNOT_DROP_2, stat).
            execute("drop view v1 restrict");
        checkViewRemainsValid();
    }

    private void testDropViewCascade() throws SQLException {
        createTestData();
        stat.execute("drop view v1 cascade");
        // v1, v2, v3 should be deleted
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).
                execute("select * from v1");
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).
                execute("select * from v2");
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, stat).
                execute("select * from v3");
        stat.execute("drop table test");
    }

    private void testCreateOrReplaceView() throws SQLException {
        createTestData();

        stat.execute("create or replace view v1 as select a as b, b as a, c from test");

        checkViewRemainsValid();
    }

    private void testCreateOrReplaceViewWithNowInvalidDependentViews()
            throws SQLException {
        createTestData();
        // v2 and v3 need more than just "c", so we should get an error
        // dependent views need more columns than just 'c'
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).
                execute("create or replace view v1 as select c from test");
        // make sure our old views come back ok
        checkViewRemainsValid();
    }

    private void testCreateOrReplaceForceViewWithNowInvalidDependentViews()
            throws SQLException {
        createTestData();

        // v2 and v3 need more than just "c",
        // but we want to force the creation of v1 anyway
        stat.execute("create or replace force view v1 as select c from test");
        // now v2 and v3 are broken, but they still exist
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, stat).
                executeQuery("select b from v2");
        stat.execute("drop table test cascade");
    }

    private void createTestData() throws SQLException {
        stat.execute("drop all objects");
        stat.execute("create table test(a int, b int, c int)");
        stat.execute("insert into test(a, b, c) values (1, 2, 3)");
        stat.execute("create view v1 as select a as b, b as a from test");
        // child of v1
        stat.execute("create view v2 as select * from v1");
        // child of v2
        stat.execute("create view v3 as select * from v2");
    }

    private void checkViewRemainsValid() throws SQLException {
        ResultSet rs = stat.executeQuery("select b from v1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("select b from v2");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("select b from test");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        stat.execute("drop table test cascade");

        ResultSet d = conn.getMetaData().getTables(null, null, null, null);
        while (d.next()) {
            // should have no tables left in the database
            assertEquals(d.getString(2) + "." + d.getString(3),
                    "INFORMATION_SCHEMA", d.getString(2));
        }
    }
}
