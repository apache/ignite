/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package android.test;

import org.h2.android.H2Database;
import org.h2.android.H2Utils;
import android.app.Activity;
import android.database.Cursor;

/**
 * Tests the Android API.
 */
public class Test extends Activity {

    public static void main(String... args) throws Exception {
        H2Database db = H2Utils.openOrCreateDatabase(
                "helloWorld.db", MODE_PRIVATE, null);
        log("opened ps=" + db.getPageSize());
        try {
            // db.execSQL("DROP TABLE IF EXISTS test");
            // log("dropped");
            db.execSQL(
                    "CREATE TABLE if not exists test(ID INTEGER PRIMARY KEY, NAME VARCHAR)");
            log("created");
            for (int j = 0; j < 10; j++) {
                Cursor c = db.rawQuery("select * from test", new String[0]);
                int count = c.getCount();
                for (int i = 0; i < count; i++) {
                    c.move(1);
                    c.getInt(0);
                    c.getString(1);
                }
                c.close();
            }
            // log("select " + count);
            db.execSQL("delete from test");
            log("delete");
            db.beginTransaction();
            for (int i = 0; i < 1000; i++) {
                db.execSQL(
                        "INSERT INTO TEST VALUES(?, 'Hello')", new Object[] { i });
            }
            db.setTransactionSuccessful();
            db.endTransaction();
            log("inserted");
            for (int i = 0; i < 10; i++) {
                Cursor c = db.rawQuery(
                        "select * from test where id=?", new String[] { "" + i });
                int count = c.getCount();
                if (count > 0) {
                    c.move(1);
                    c.getInt(0);
                    c.getString(1);
                }
                c.close();
            }
            log("select");
        } finally {
            db.close();
            log("closed");
        }
    }

    private static void log(String s) {
        System.out.println(s);
    }

}
