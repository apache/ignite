/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.UUID;

import org.h2.engine.SysProperties;
import org.h2.test.TestBase;

/**
 * Tests java object values when SysProperties.SERIALIZE_JAVA_OBJECT property is
 * disabled.
 *
 * @author Sergi Vladykin
 */
public class TestJavaObject extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = createCaller().init();
        test.config.traceTest = true;
        test.config.memory = true;
        test.config.networked = true;
        test.config.beforeTest();
        test.test();
        test.config.afterTest();
    }

    @Override
    public void test() throws Exception {
        SysProperties.serializeJavaObject = false;
        try {
            trace("Test Java Object");
            doTest(new MyObj(1), new MyObj(2), false);
            doTest(Arrays.asList(UUID.randomUUID(), null),
                    Arrays.asList(UUID.randomUUID(), UUID.randomUUID()), true);
            // doTest(new Timestamp(System.currentTimeMillis()),
            //         new Timestamp(System.currentTimeMillis() + 10000),
            //         false);
            doTest(200, 100, false);
            doTest(200, 100L, true);
            // doTest(new Date(System.currentTimeMillis() + 1000),
            //         new Date(System.currentTimeMillis()), false);
            // doTest(new java.util.Date(System.currentTimeMillis() + 1000),
            //         new java.util.Date(System.currentTimeMillis()), false);
            // doTest(new Time(System.currentTimeMillis() + 1000),
            //         new Date(System.currentTimeMillis()), false);
            // doTest(new Time(System.currentTimeMillis() + 1000),
            //         new Timestamp(System.currentTimeMillis()), false);
        } finally {
            SysProperties.serializeJavaObject = true;
        }
    }

    private void doTest(Object o1, Object o2, boolean hash) throws SQLException {
        deleteDb("javaObject");
        Connection conn = getConnection("javaObject");
        Statement stat = conn.createStatement();
        stat.execute("create table t(id identity, val other)");

        PreparedStatement ins = conn.prepareStatement(
                "insert into t(val) values(?)");

        ins.setObject(1, o1, Types.JAVA_OBJECT);
        assertEquals(1, ins.executeUpdate());

        ins.setObject(1, o2, Types.JAVA_OBJECT);
        assertEquals(1, ins.executeUpdate());

        ResultSet rs = stat.executeQuery(
                "select val from t order by val limit 1");

        assertTrue(rs.next());

        Object smallest;
        if (hash) {
            if (o1.getClass() != o2.getClass()) {
                smallest = o1.getClass().getName().compareTo(
                        o2.getClass().getName()) < 0 ? o1 : o2;
            } else {
                assertFalse(o1.hashCode() == o2.hashCode());
                smallest = o1.hashCode() < o2.hashCode() ? o1 : o2;
            }
        } else {
            @SuppressWarnings("unchecked")
            int compare = ((Comparable<Object>) o1).compareTo(o2);
            assertFalse(compare == 0);
            smallest = compare < 0 ? o1 : o2;
        }

        assertEquals(smallest.toString(), rs.getString(1));

        Object y = rs.getObject(1);

        assertTrue(smallest.equals(y));
        assertFalse(rs.next());
        rs.close();

        PreparedStatement prep = conn.prepareStatement(
                "select id from t where val = ?");

        prep.setObject(1, o1, Types.JAVA_OBJECT);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();

        prep.setObject(1, o2, Types.JAVA_OBJECT);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();

        stat.close();
        prep.close();

        conn.close();
        deleteDb("javaObject");
        // trace("ok: " + o1.getClass().getName() + " vs " +
        // o2.getClass().getName());
    }

    /**
     * A test class.
     */
    public static class MyObj implements Comparable<MyObj>, Serializable {

        private static final long serialVersionUID = 1L;
        private final int value;

        MyObj(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "myObj:" + value;
        }

        @Override
        public int compareTo(MyObj o) {
            return value - o.value;
        }

        @Override
        public boolean equals(Object o) {
            return toString().equals(o.toString());
        }

        @Override
        public int hashCode() {
            return -value;
        }

    }
}
