/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import org.h2.api.JavaObjectSerializer;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;

/**
 * Tests {@link JavaObjectSerializer}.
 *
 * @author Sergi Vladykin
 * @author Davide Cavestro
 */
public class TestJavaObjectSerializer extends TestBase {

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
        deleteDb("javaSerializer");
        testStaticGlobalSerializer();
        testDbLevelJavaObjectSerializer();
        deleteDb("javaSerializer");
    }

    private void testStaticGlobalSerializer() throws Exception {
        JdbcUtils.serializer = new JavaObjectSerializer() {
            @Override
            public byte[] serialize(Object obj) throws Exception {
                assertEquals(100500, ((Integer) obj).intValue());

                return new byte[] { 1, 2, 3 };
            }

            @Override
            public Object deserialize(byte[] bytes) throws Exception {
                assertEquals(new byte[] { 1, 2, 3 }, bytes);

                return 100500;
            }
        };

        try {
            deleteDb("javaSerializer");
            Connection conn = getConnection("javaSerializer");

            Statement stat = conn.createStatement();
            stat.execute("create table t(id identity, val other)");

            PreparedStatement ins = conn.prepareStatement("insert into t(val) values(?)");

            ins.setObject(1, 100500, Types.JAVA_OBJECT);
            assertEquals(1, ins.executeUpdate());

            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select val from t");

            assertTrue(rs.next());

            assertEquals(100500, ((Integer) rs.getObject(1)).intValue());
            assertEquals(new byte[] { 1, 2, 3 }, rs.getBytes(1));

            conn.close();
            deleteDb("javaSerializer");
        } finally {
            JdbcUtils.serializer = null;
        }
    }

    /**
     * Tests per-database serializer when set through the related SET command.
     */
    public void testDbLevelJavaObjectSerializer() throws Exception {

        DbLevelJavaObjectSerializer.testBaseRef = this;

        try {
            deleteDb("javaSerializer");
            Connection conn = getConnection("javaSerializer");

            conn.createStatement().execute("SET JAVA_OBJECT_SERIALIZER '"+
                    DbLevelJavaObjectSerializer.class.getName()+"'");

            Statement stat = conn.createStatement();
            stat.execute("create table t1(id identity, val other)");

            PreparedStatement ins = conn.prepareStatement("insert into t1(val) values(?)");

            ins.setObject(1, 100500, Types.JAVA_OBJECT);
            assertEquals(1, ins.executeUpdate());

            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select val from t1");

            assertTrue(rs.next());

            assertEquals(100500, ((Integer) rs.getObject(1)).intValue());
            assertEquals(new byte[] { 1, 2, 3 }, rs.getBytes(1));

            conn.close();
            deleteDb("javaSerializer");
        } finally {
            DbLevelJavaObjectSerializer.testBaseRef = null;
        }
    }

    /**
     * The serializer to use for this test.
     */
    public static class DbLevelJavaObjectSerializer implements
            JavaObjectSerializer {

        /**
         * The test.
         */
        static TestBase testBaseRef;

        @Override
        public byte[] serialize(Object obj) throws Exception {
            testBaseRef.assertEquals(100500, ((Integer) obj).intValue());

            return new byte[] { 1, 2, 3 };
        }

        @Override
        public Object deserialize(byte[] bytes) throws Exception {
            testBaseRef.assertEquals(new byte[] { 1, 2, 3 }, bytes);

            return 100500;
        }

    }
}
