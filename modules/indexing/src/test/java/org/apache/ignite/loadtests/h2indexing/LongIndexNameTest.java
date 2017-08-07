package org.apache.ignite.loadtests.h2indexing;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 *
 */
public class LongIndexNameTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPersistentStoreConfiguration(new PersistentStoreConfiguration())
            .setCacheConfiguration(new <String, Person>CacheConfiguration("cache")
                .setQueryEntities(getIndexCfg()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        deleteWorkFiles();
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            IgniteCache cache = insertSomeData(ignite);

            QueryCursor cursor1 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name like '%Name 0'"));
            QueryCursor cursor1Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name = 'Name 0'"));

            QueryCursor cursor2 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age like '%0'"));
            QueryCursor cursor2Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age = 0"));

            assertEquals(cursor1.getAll().size(), cursor1Idx.getAll().size());
            assertEquals(cursor2.getAll().size(), cursor2Idx.getAll().size());

            ignite.close();

            Thread.sleep(2_000);

            ignite = startGrid(0);

            cache = insertSomeData(ignite);

            cursor1 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name like '%Name 0'"));
            cursor1Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name = 'Name 0'"));

            cursor2 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age like '%0'"));
            cursor2Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age = 0"));

            assertEquals(cursor1.getAll().size(), cursor1Idx.getAll().size());
            assertEquals(cursor2.getAll().size(), cursor2Idx.getAll().size());


        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @NotNull private IgniteCache insertSomeData(Ignite ignite) {
        if (!ignite.active())
            ignite.active(true);

        IgniteCache cache = ignite.cache("cache");

        for (int i=0; i<10; i++)
            cache.put(String.valueOf(System.currentTimeMillis()), new Person("Name " + i, i));
        return cache;
    }

    /**
     *
     */
    public static List<QueryEntity> getIndexCfg() {
        ArrayList<QueryEntity> entities = new ArrayList<>();

        QueryEntity qe = new QueryEntity(String.class.getName(), Person.class.getName());

        LinkedHashMap<String, String> fieldsMap = new LinkedHashMap<>();
        fieldsMap.put("name", String.class.getName());
        fieldsMap.put("age", Integer.class.getName());

        qe.setFields(fieldsMap);

        ArrayList<QueryIndex> indices = new ArrayList<>();
        QueryIndex index = new QueryIndex("name", true, "LONG_NAME_123456789012345678901234567890" +
            "12345678901234567890123456789012345678901234567890123456789012345678901234567890");
        QueryIndex index2 = new QueryIndex("age", true, "AGE_IDX");
        indices.add(index);
        indices.add(index2);

        qe.setIndexes(indices);

        entities.add(qe);

        return entities;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     *
     */
    private static class Person {
        private String name;

        private int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
