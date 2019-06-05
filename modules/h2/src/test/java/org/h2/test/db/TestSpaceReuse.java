/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests if disk space is reused after deleting many rows.
 */
public class TestSpaceReuse extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("spaceReuse");
        long max = 0, now = 0, min = Long.MAX_VALUE;
        for (int i = 0; i < 20; i++) {
            Connection conn = getConnection("spaceReuse");
            Statement stat = conn.createStatement();
            stat.execute("set retention_time 0");
            stat.execute("create table if not exists t(i int)");
            stat.execute("insert into t select x from system_range(1, 500)");
            conn.close();
            conn = getConnection("spaceReuse");
            conn.createStatement().execute("delete from t");
            conn.close();
            String fileName = getBaseDir() + "/spaceReuse";
            if (Constants.VERSION_MINOR >= 4) {
                fileName += Constants.SUFFIX_MV_FILE;
            } else {
                fileName += Constants.SUFFIX_PAGE_FILE;
            }
            now = FileUtils.size(fileName);
            assertTrue(now > 0);
            if (i < 10) {
                max = Math.max(max, now);
            } else {
                min = Math.min(min, now);
            }
        }
        assertTrue("min: " + min + " max: " + max, min <= max);
        deleteDb("spaceReuse");
    }

}
