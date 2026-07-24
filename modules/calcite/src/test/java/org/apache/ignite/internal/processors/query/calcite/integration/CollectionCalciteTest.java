package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import org.apache.ignite.cache.query.annotations.QuerySqlTableFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

/** */
public class CollectionCalciteTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setSqlFunctionClasses(Functions.class);

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(ccfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("create table PUBLIC.PERSON(id int primary key, name varchar, age int)");

        for (int i = 1; i <= 10; i++)
            sql("insert into PUBLIC.PERSON(id, name, age) values(?, ?, ?)", i, "foo" + i, (i % 3) + 18);
    }

    /** */
    @Test
    public void test0() {
        assertQuery("SELECT u.name\n" +
            "FROM UNNEST(\n" +
            "  SELECT ARRAY_AGG(DISTINCT name ORDER BY name)\n" +
            "  FROM PUBLIC.PERSON\n" +
            ") u(name)")
            .returns("foo1")
            .returns("foo10")
            .returns("foo2")
            .returns("foo3")
            .returns("foo4")
            .returns("foo5")
            .returns("foo6")
            .returns("foo7")
            .returns("foo8")
            .returns("foo9")
            .check();
    }

    /** */
    @Test
    public void test1() {
        List<?> names = (List<?>)sql("SELECT ARRAY_AGG(DISTINCT name ORDER BY name) FROM PUBLIC.PERSON").get(0).get(0);

        assertQuery("SELECT u.name FROM TABLE(FROM_STRING_ARRAY(?)) u")
            .withParams(names)
            .returns("foo1")
            .returns("foo10")
            .returns("foo2")
            .returns("foo3")
            .returns("foo4")
            .returns("foo5")
            .returns("foo6")
            .returns("foo7")
            .returns("foo8")
            .returns("foo9")
            .check();
    }

    @Test
    public void name() {
        List<List<?>> sql = sql("SELECT CAST(\n" +
            "  MULTISET(\n" +
            "    SELECT id, name, age\n" +
            "    FROM person\n" +
            "  )\n" +
            "  AS ROW(id INTEGER, name VARCHAR, age INTEGER) MULTISET\n" +
            ") AS persons");

        System.out.println(sql);
    }

    /** */
    public static class Functions {
        /** */
        @QuerySqlTableFunction(alias = "FROM_STRING_ARRAY", columnTypes = {String.class}, columnNames = {"NAME"})
        public static List<Object[]> fromStringArray(List<String> arr) {
            return arr.stream()
                .map(s -> new Object[] {s})
                .collect(toList());
        }
    }
}
