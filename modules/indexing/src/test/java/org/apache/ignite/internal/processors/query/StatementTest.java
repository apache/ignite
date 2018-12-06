package org.apache.ignite.internal.processors.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.ignite.Ignite;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class StatementTest extends GridCommonAbstractTest {

    private static Connection conn;
    private static Connection conn1;

    public StatementTest() {
        super(true);
    }

    public void test() throws Exception {
        String selectSql = "SELECT * FROM city";

        runQuery(selectSql);
    }

    public void afterTest() throws Exception {
        conn.close();
        conn1.close();
        super.afterTest();
    }

    public void beforeTest() throws Exception {
        super.beforeTest();
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        final String dbUrl =
            "jdbc:ignite:thin://localhost;lazy=true;skipReducerOnUpdate=true;replicatedOnly=true";
        final Properties props = new Properties();
        conn = DriverManager.getConnection(dbUrl, props);
        conn1 = DriverManager.getConnection(dbUrl, props);
        initData();
    }

    private void initData() throws SQLException {
        conn.createStatement().execute("CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) WITH \"template=replicated\"");
        for (int i = 0; i < 80000; i++) {
            String s = String.valueOf(Math.random());
            conn.prepareStatement("insert into city(id,name) VALUES(" + i + "," + s + ")").execute();
        }
    }

    private volatile boolean stop = false;
    public void runQuery(final String sql) throws Exception {

        Thread th = new Thread(new Runnable() {

            @Override
            public void run() {
                long startTime = System.currentTimeMillis();
                while (!stop) {

                    try (Statement stmt = conn.createStatement()) {
                        try (ResultSet rs = stmt.executeQuery(sql)) {

                            ResultSetMetaData rsmd = rs.getMetaData();
                            int colCount = rsmd.getColumnCount();
                            int count = 1;
                            try {
                                while (rs.next()) {
                                    //                                                                      System.out.print("conn ");
                                    for (int i = 1; i <= colCount; i++) {
                                        //                                                                                System.out.print(rsmd.getColumnName(i) + ":" + rs.getObject(i) + "");
                                    }
                                    //                                                                        System.out.println(count);
                                    count++;
                                }
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                                System.out.println(System.currentTimeMillis() - startTime);
                                System.out.println(e);
                                System.out.println(rs.isClosed());
                                System.out.println(stmt.isClosed());
                                System.out.println(conn.isClosed());
                                System.exit(-1);
                            }
                            System.out.println("test");

                        }
                    }
                    catch (Exception e) {
                        System.out.println(e);
                    }
                }
            }
        });
        th.start();

        Thread th1 = new Thread(new Runnable() {

            @Override
            public void run() {
                long startTime = System.currentTimeMillis();
                while (!stop) {

                    try (Statement stmt = conn1.createStatement()) {
                        try (ResultSet rs = stmt.executeQuery(sql)) {

                            ResultSetMetaData rsmd = rs.getMetaData();
                            int colCount = rsmd.getColumnCount();
                            int count = 1;
                            try {
                                while (rs.next()) {
                                    //                                                                        System.out.print("conn1 ");
                                    for (int i = 1; i <= colCount; i++) {
                                        //                                                                                System.out.print(rsmd.getColumnName(i) + ":" + rs.getObject(i) + "");
                                    }
                                    //                                                                        System.out.println(count);
                                    count++;
                                }
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                                System.out.println(System.currentTimeMillis() - startTime);
                                System.out.println(e);
                                System.out.println(rs.isClosed());
                                System.out.println(stmt.isClosed());
                                System.out.println(conn.isClosed());
                                System.exit(-1);
                            }
                            System.out.println("test");

                        }
                    }
                    catch (Exception e) {
                        System.out.println(e);
                    }
                }
            }
        });
        th1.start();

        Thread.sleep(30000);
        assertTrue(th.isAlive());
        assertTrue(th1.isAlive());
        stop = true;
        th.join();
        th1.join();
    }
}
