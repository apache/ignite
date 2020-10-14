package org.apache.ignite.index;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.index.IgniteIndexing;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


// TODO: add check that get compare using only inlined fields (check that cache is clear?)
public class QueryIndexTest extends GridCommonAbstractTest {
    /** */
    protected IgniteIndexing idx;

    /** */
    @Before
    public void setUp() throws Exception {
        idx = new IgniteIndexing();
    }

    /** */
    @After
    public void tearDown() throws Exception {
        stopAllGrids();
    }

    @Test
    public void testStrings() throws Exception {
        IgniteEx ignite = startGrid();

        IgniteCache<Long, Person> cache = ignite.getOrCreateCache("CACHE");

        cache.put(0L, new Person(0L, "Maksim0"));

        // TODO: How does "use index" work? Check that query touch index?
        List<List<?>> result = cache
            .query(new SqlFieldsQuery("select _key, _val from Person where name = 'Maksim0'"))
            .getAll();

        System.out.println(result);

        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
        assertEquals(0L, result.get(0).get(0));
        assertEquals(new Person(0L, "Maksim0"), result.get(0).get(1));

//        MultiSortedIndex<String> idx = createIndex(ignite, func1);

/*
            // Check empty index.
        assertNull(idx.get(new Object[]{ "One" }));

        // Check put item
        cache.put(1, "One");
        cache.put(2, "Two");

        assertEquals("One", idx.get(new Object[]{ "One" }));
        assertEquals("Two", idx.get(new Object[]{ "Two" }));

        // Check update item
        cache.put(1, "NewOne");
        assertNull(idx.get(new Object[]{ "One" }));
        assertEquals("NewOne", idx.get(new Object[]{ "NewOne" }));
        assertEquals("Two", idx.get(new Object[]{ "Two" }));

        // Check remove item
        cache.remove(1);
        assertNull(idx.get(new Object[]{ "NewOne" }));
        assertEquals("Two", idx.get(new Object[]{ "Two" }));
*/
    }

//    /** */
//    protected <CV> MultiSortedIndex<CV> createIndex(IgniteEx node) {
//        GridCacheContext cctx = node.cachex("CACHE").context();
//
//        QueryIndexSchema schema = new QueryIndexSchema();
//
//        QueryIndexDefinition def =
//            new QueryIndexDefinition(cctx, "idx", 1, schema);
//
//        return (MultiSortedIndex<CV>) idx
//            .createIndex(new InlineIndexFactory(), def)
//            .unwrap(MultiSortedIndex.class);
//    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setIndexingSpi(idx);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName("CACHE")
            .setIndexedTypes(Long.class, Person.class)
            .setQueryParallelism(1));

        return cfg;
    }

    public static class Person implements Serializable {
        /** Indexed field. Will be visible to the SQL engine. */
        @QuerySqlField
        private long id;

        /** Queryable field. Will be visible to the SQL engine. */
        @QuerySqlField(index = true, inlineSize = 20)
        private String name;

        Person(long id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override public boolean equals(Object other) {
            Person o = (Person) other;

            return id == o.id && name.equals(o.name);
        }

    }
}
