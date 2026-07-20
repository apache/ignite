package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.sql.SqlIndexView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MixedIndexConfigurationTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "mixed-index-cache";

    /** */
    private static final String COMPOSITE_IDX_NAME = "PERSON_NAME_AGE_IDX";

    /** */
    private static final String INDEXES_VIEW = "indexes";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Person.class)
            .setQueryEntities(Collections.singletonList(configuredPersonEntity()));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Verifies that the node starts and the cache works, but the composite index configured through QueryEntity is
     * silently omitted.
     */
    @Test
    public void testConfiguredCompositeIndexIsIgnored() throws Exception {
        IgniteEx node = startGrid(0);

        awaitPartitionMapExchange();

        // Node successfully joined the cluster
        assertEquals(1, node.cluster().forServers().nodes().size());

        IgniteCache<Integer, Person> cache = node.cache(CACHE_NAME);

        assertNotNull(cache);

        String name = "Alice";
        int age = 22;

        // Cache is operational despite partially ignored SQL configuration
        cache.put(1, new Person(name, age));

        Person person = cache.get(1);

        assertNotNull(person);
        assertEquals(name, person.name);
        assertEquals(age, person.age);

        List<SqlIndexView> indexes = cacheIndexes(node);

        String annotationIdxName = QueryUtils.normalizeObjectName(annotationIndexName(), false);

        assertTrue("Annotation-based index was not created",
            indexes.stream().anyMatch(idx -> annotationIdxName.equals(idx.indexName())));

        assertFalse("Configured composite index unexpectedly exists",
            indexes.stream().anyMatch(idx -> COMPOSITE_IDX_NAME.equals(idx.indexName())));
    }

    /** */
    private static QueryEntity configuredPersonEntity() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("name", String.class.getName());
        fields.put("age", Integer.class.getName());

        QueryIndex compositeIdx = new QueryIndex(
            Arrays.asList("name", "age"),
            QueryIndexType.SORTED
        ).setName(COMPOSITE_IDX_NAME);

        return new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Person.class.getName())
            .setTableName(Person.class.getSimpleName())
            .setFields(fields)
            .setIndexes(Collections.singletonList(compositeIdx));
    }

    /** */
    private static List<SqlIndexView> cacheIndexes(IgniteEx node) {
        SystemView<SqlIndexView> indexes = node.context().systemView().view(INDEXES_VIEW);

        assertNotNull(indexes);

        List<SqlIndexView> res = new ArrayList<>();

        for (SqlIndexView idx : indexes) {
            if (CACHE_NAME.equals(idx.cacheName()))
                res.add(idx);
        }

        return res;
    }

    /** */
    private static String annotationIndexName() {
        QueryEntity entity = new QueryEntity(Integer.class, Person.class);

        QueryIndex idx = entity.getIndexes().iterator().next();

        return QueryUtils.indexName(entity, idx);
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        private final String name;

        /** */
        @QuerySqlField
        private final int age;

        /** */
        private Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
