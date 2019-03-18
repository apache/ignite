/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.h2.tools.DeleteDbFiles;

/**
 * A very simple class that shows how to load the driver, create a database,
 * create a table, and insert some data.
 */
public class ToDate {

    /**
     * Called when ran from command line.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {

        // delete the database named 'test' in the user home directory
        DeleteDbFiles.execute("~", "test", true);

        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
        Statement stat = conn.createStatement();

        stat.execute("create table ToDateTest(id int primary key, " +
                "start_date datetime, end_date datetime)");
        stat.execute("insert into ToDateTest values(1, "
                + "ADD_MONTHS(TO_DATE('2015-11-13', 'yyyy-MM-DD'), 1), "
                + "TO_DATE('2015-12-15', 'YYYY-MM-DD'))");
        stat.execute("insert into ToDateTest values(1, " +
                "TO_DATE('2015-11-13', 'yyyy-MM-DD'), " +
                "TO_DATE('2015-12-15', 'YYYY-MM-DD'))");
        stat.execute("insert into ToDateTest values(2, " +
                "TO_DATE('2015-12-12 00:00:00', 'yyyy-MM-DD HH24:MI:ss'), " +
                "TO_DATE('2015-12-16 15:00:00', 'YYYY-MM-DD HH24:MI:ss'))");
        stat.execute("insert into ToDateTest values(3, " +
                "TO_DATE('2015-12-12 08:00 A.M.', 'yyyy-MM-DD HH:MI AM'), " +
                "TO_DATE('2015-12-17 08:00 P.M.', 'YYYY-MM-DD HH:MI AM'))");
        stat.execute("insert into ToDateTest values(4, " +
                "TO_DATE(substr('2015-12-12 08:00 A.M.', 1, 10), 'yyyy-MM-DD'), " +
                "TO_DATE('2015-12-17 08:00 P.M.', 'YYYY-MM-DD HH:MI AM'))");

        ResultSet rs = stat.executeQuery("select * from ToDateTest");
        while (rs.next()) {
            System.out.println("Start date: " + dateToString(rs.getTimestamp("start_date")));
            System.out.println("End date: " + dateToString(rs.getTimestamp("end_date")));
            System.out.println();
        }
        stat.close();
        conn.close();
    }

    private static String dateToString(Date date) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
    }

}
