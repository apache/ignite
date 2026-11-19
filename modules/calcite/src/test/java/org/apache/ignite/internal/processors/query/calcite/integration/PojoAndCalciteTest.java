package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlTableFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

/** */
public class PojoAndCalciteTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setSqlFunctionClasses(Functions.class);

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(cacheCfg);
    }

    /** */
    @Test
    public void testToFooPersonWithoutTable() {
        assertQuery("select to_foo_person(1, 'foo', 20)")
            .returns(new FooPerson(1, "foo", 20))
            .check();

        assertQuery("select to_foo_person(?, ?, ?)")
            .withParams(2, "bar", "30")
            .returns(new FooPerson(2, "bar", 30))
            .check();
    }

    /** */
    @Test
    public void testToFooPersonWithTable() {
        sql("create table PUBLIC.PERSON(id int primary key, name varchar, age int)");

        for (int i = 1; i <= 3; i++)
            sql("insert into PUBLIC.PERSON(id, name, age) values(?, ?, ?)", i, "foo" + i, i * 10);

        assertQuery("select to_foo_person(id, name, age) from PUBLIC.PERSON")
            .returns(new FooPerson(1, "foo1", 10))
            .returns(new FooPerson(2, "foo2", 20))
            .returns(new FooPerson(3, "foo3", 30))
            .check();
    }

    /** */
    @Test
    public void testToFooPersonWithTableAndCollectionFunction() {
        sql("create table PUBLIC.PERSON(id int primary key, name varchar, age int)");

        for (int i = 1; i <= 3; i++)
            sql("insert into PUBLIC.PERSON(id, name, age) values(?, ?, ?)", i, "foo" + i, i * 10);

        assertQuery("select array_agg(to_foo_person(id, name, age)) from PUBLIC.PERSON")
            .returns(List.of(new FooPerson(1, "foo1", 10), new FooPerson(2, "foo2", 20), new FooPerson(3, "foo3", 30)))
            .check();
    }

    /** */
    @Test
    public void testFromFooPersons() {
        assertQuery("select * from table(from_foo_persons(array[to_foo_person(1, 'foo', 20), to_foo_person(2, 'bar', 30)]))")
            .returns(1, "foo", 20)
            .returns(2, "bar", 30)
            .check();

        assertQuery("select * from table(from_foo_persons(?))")
            .withParams(List.of(new FooPerson(1, "foo", 20), new FooPerson(2, "bar", 30)))
            .returns(1, "foo", 20)
            .returns(2, "bar", 30)
            .check();
    }

    /** */
    public static class Functions {
        /** */
        @QuerySqlFunction(alias = "TO_FOO_PERSON")
        public FooPerson toFooPerson(int id, String name, int age) {
            return new FooPerson(id, name, age);
        }

        /** */
        @QuerySqlTableFunction(
            alias = "FROM_FOO_PERSONS",
            columnTypes = {int.class, String.class, int.class},
            columnNames = {"ID", "NAME", "AGE"}
        )
        public List<Object[]> fromFooPersons(List<FooPerson> persons) {
            return persons.stream()
                .map(p -> new Object[] {p.id, p.name, p.age})
                .collect(toList());
        }
    }

    /** */
    public static class FooPerson {
        public final int id;
        public final String name;
        public final int age;

        /** */
        public FooPerson(int id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass())
                return false;

            FooPerson person = (FooPerson) o;
            return id == person.id && age == person.age && Objects.equals(name, person.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;
            result = 31 * result + Objects.hashCode(name);
            result = 31 * result + age;
            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FooPerson.class, this);
        }
    }
}
