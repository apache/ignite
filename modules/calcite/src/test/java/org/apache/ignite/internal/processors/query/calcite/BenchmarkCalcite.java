
package org.apache.ignite.internal.processors.query.calcite;

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BenchmarkCalcite extends GridCommonAbstractTest {
    /** */
    private IgniteEx ignite;

    /** */
    IgniteEx client;

    @Before
    public void setup() throws Exception {
        ignite = startGrids(2);

        //client = startClientGrid();
    }

    @After
    public void tearDown() {
        stopAllGrids();
    }

    @Test
    public void test() throws Exception {
        IgniteCache<Integer, MyClass> cache1 = ignite.getOrCreateCache(new CacheConfiguration<Integer, MyClass>()
            .setName("tbl1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, MyClass.class).setTableName("TBL1")))
            .setBackups(1)
        );

        waitForReadyTopology(internalCache(cache1).context().topology(), new AffinityTopologyVersion(2, 2));

        System.err.println("wait:");

        // populate
        for (int i = 0; i < 10_000; ++i)
            cache1.put(i, new MyClass("some data " + i, i));

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        System.err.println("wait2:");

        int cnt = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < 10_000; ++i) {
            List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
                "select * from TBL1 d where d.amount = ?", i);

            Iterator it;
            for (FieldsQueryCursor c : query) {
                it = c.iterator();
                while (it.hasNext()) {
                    cnt++;
                    it.next();
                }
            }

            query.forEach((q) -> q.close());

/*            FieldsQueryCursor<List<?>> query = cache1.query(new SqlFieldsQuery("select * from PUBLIC.TBL1 d where d" +
                ".amount = ?").setArgs(i));

            Iterator<List<?>> it = query.iterator();

            while (it.hasNext()) {
                cnt++;
                it.next();
            }

            query.close();*/

        }

        System.err.println("Complete time: " + (System.currentTimeMillis() - start) + "  " + cnt);

        ignite.cluster().state(ClusterState.INACTIVE);
    }

    @Test
    public void test2() throws Exception {
        IgniteCache<Integer, MyClass> cache1 = ignite.getOrCreateCache(new CacheConfiguration<Integer, MyClass>()
            .setName("tbl1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, MyClass.class).setTableName("TBL1")))
            .setBackups(1)
        );

        waitForReadyTopology(internalCache(cache1).context().topology(), new AffinityTopologyVersion(2, 2));

        System.err.println("wait:");

        // populate
        int mul = 1;

        while (mul <= 100_000) {
            for (int i = mul; i < mul * 10; ++i)
                cache1.put(i, new MyClass("some data " + i, mul));

            mul *= 10;
        }

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        System.err.println("wait2:");

        int cnt = 0;

        long start = System.currentTimeMillis();

        for (int i = 1; i <= 100_000; i *= 10) {
/*            List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
                "select * from TBL1 d where d.amount = ?", i);

            Iterator it;
            for (FieldsQueryCursor c : query) {
                it = c.iterator();
                while (it.hasNext()) {
                    cnt++;
                    it.next();
                }
            }

            query.forEach((q) -> q.close());*/

            FieldsQueryCursor<List<?>> query = cache1.query(new SqlFieldsQuery("select * from PUBLIC.TBL1 d where d" +
                ".amount = ?").setArgs(i));

            Iterator<List<?>> it = query.iterator();

            while (it.hasNext()) {
                cnt++;
                it.next();
            }

            query.close();
        }

        System.err.println("Complete time: " + (System.currentTimeMillis() - start) + "  " + cnt);

        ignite.cluster().state(ClusterState.INACTIVE);
    }

    //@Test
    public void test3() throws Exception {
        //client = startClientGrid();

        IgniteCache<Integer, MyClass> cache1 = ignite.getOrCreateCache(new CacheConfiguration<Integer, MyClass>()
            .setName("tbl1")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, MyClass.class).setTableName("TBL1")))
            .setBackups(1)
        );

        IgniteCache<Integer, MyClass2> cache2 = ignite.getOrCreateCache(new CacheConfiguration<Integer, MyClass2>()
            .setName("tbl2")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, MyClass2.class).setTableName("TBL2")))
            .setBackups(1)
        );

        waitForReadyTopology(internalCache(cache1).context().topology(), new AffinityTopologyVersion(2, 3));

        System.err.println("wait:");

        // populate
        for (int i = 0; i < 10_00; ++i) {
            cache1.put(i, new MyClass("some data " + i, i));

            //cache2.put(i, new MyClass2("other data " + i, i));
        }

        for (int i = 0; i < 10_00; ++i) {
            //cache1.put(i, new MyClass("some data " + i, i));

            cache2.put(i, new MyClass2("other data " + i, i));
        }

        QueryEngine engine = Commons.lookupComponent(grid(1).context(), QueryEngine.class);

        System.err.println("wait2:");

        int cnt = 0;

        long start = System.currentTimeMillis();

/*        List<FieldsQueryCursor<List<?>>> query0 = engine.query(null, "PUBLIC",
            "select * from TBL1 where amount > 10");

        Iterator it0;
        for (FieldsQueryCursor c : query0) {
            it0 = c.iterator();
            while (it0.hasNext()) {
                cnt++;
                Object r = it0.next();
            }
        }*/

//EXPLAIN PLAN FOR

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC",
            "select t1.amount from TBL1 t1 LEFT JOIN TBL2 t2 ON t1.amount = t2.amount2 where t1" +
                ".amount2 > 10");

        System.err.println(query.get(0).getAll().size());

/*        Iterator it;
        for (FieldsQueryCursor c : query) {
            it = c.iterator();
            while (it.hasNext()) {
                cnt++;
                Object r = it.next();
            }
        }

        query.forEach((q) -> q.close());*/

/*        FieldsQueryCursor<List<?>> query = cache1.query(new SqlFieldsQuery("select t1.amount from TBL1 t1 LEFT JOIN TBL2 t2 ON t1.amount = t2.amount2 where t1" +
                ".amount2 > 10"));

        System.err.println(query.getAll().size());*/

/*        Iterator<List<?>> it = query.iterator();

        while (it.hasNext()) {
            cnt++;
            it.next();
        }

        query.close();*/

        System.err.println("Complete time: " + (System.currentTimeMillis() - start) + "  " + cnt);

        ignite.cluster().state(ClusterState.INACTIVE);
    }

    /** */
    public static class MyClass {
        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField(index = true)
        @AffinityKeyMapped
        public Integer amount;

        @QuerySqlField
        public Integer amount1;

        @QuerySqlField
        public Integer amount2;

        /** */
        public MyClass(String name, Integer amount) {
            this.name = name;
            this.amount = amount;
            this.amount1 = amount;
            this.amount2 = amount;
        }
    }

    /** */
    public static class MyClass2 {
        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField
        @AffinityKeyMapped
        public Integer amount2;

        /** */
        public MyClass2(String name, Integer amount) {
            this.name = name;
            this.amount2 = amount;
        }
    }
}
