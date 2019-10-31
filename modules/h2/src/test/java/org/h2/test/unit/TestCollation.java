/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Test the ICU4J collator.
 */
public class TestCollation extends TestBase {

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
        deleteDb("collation");
        Connection conn = getConnection("collation");
        Statement stat = conn.createStatement();
        assertThrows(ErrorCode.INVALID_VALUE_2, stat).
                execute("set collation xyz");
        stat.execute("set collation en");
        stat.execute("set collation default_en");
        assertThrows(ErrorCode.CLASS_NOT_FOUND_1, stat).
                execute("set collation icu4j_en");

        stat.execute("set collation ge");
        stat.execute("create table test(id int)");
        // the same as the current - ok
        stat.execute("set collation ge");
        // not allowed to change now
        assertThrows(ErrorCode.COLLATION_CHANGE_WITH_DATA_TABLE_1, stat).
            execute("set collation en");

        conn.close();
        deleteDb("collation");
    }

}
