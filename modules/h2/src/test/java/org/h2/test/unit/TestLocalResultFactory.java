/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.LocalResult;
import org.h2.result.LocalResultFactory;
import org.h2.test.TestBase;

/**
 * Test {@link LocalResultFactory} setting.
 */
public class TestLocalResultFactory extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:localResultFactory;LOCAL_RESULT_FACTORY=\""
            + MyTestLocalResultFactory.class.getName() + '"')) {
            Statement stat = conn.createStatement();

            stat.execute("create table t1(id int, name varchar)");
            for (int i = 0; i < 1000; i++) {
                stat.execute("insert into t1 values(" + i + ", 'name')");
            }
            assertEquals(MyTestLocalResultFactory.COUNTER.get(), 0);

            stat.execute("select * from t1");
            assertEquals(MyTestLocalResultFactory.COUNTER.get(), 1);
        }
    }

    /**
     * Test local result factory.
     */
    public static class MyTestLocalResultFactory extends LocalResultFactory {
        /** Call counter for the factory methods. */
        static final AtomicInteger COUNTER = new AtomicInteger();

        @Override public LocalResult create(Session session, Expression[] expressions, int visibleColumnCount) {
            COUNTER.incrementAndGet();
            return LocalResultFactory.DEFAULT.create(session, expressions, visibleColumnCount);
        }

        @Override public LocalResult create() {
            COUNTER.incrementAndGet();
            return LocalResultFactory.DEFAULT.create();
        }
    }
}
