/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import javax.cache.CacheException;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertArrayEquals;

/** Tests query entity configuration in {@link CacheConfiguration}. */
public class CacheConfigurationQueryEntityTest extends GridCommonAbstractTest {
    /** */
    private static final String MIXED_QUERY_ENTITIES_API_ERROR =
        "Query entities can be configured either with setIndexedTypes or setQueryEntities, but not both [cacheName=%s]";

    /** */
    private static final String CACHE_NAME = "query-entity-merge-cache";

    /** */
    private static final String NAME_FIELD = "name";

    /** */
    private static final String AGE_FIELD = "age";

    /** */
    private boolean staticCfg;

    /** */
    private boolean indexedTypesCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (staticCfg) {
            CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

            if (indexedTypesCfg)
                ccfg.setIndexedTypes(Integer.class, Person.class);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Verifies that a repeated {@link CacheConfiguration#setQueryEntities} call replaces query entities
     * configured by the previous call.
     */
    @Test
    public void testRepeatedSetQueryEntitiesReplacesPreviousEntities() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        QueryEntity first = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Person.class.getName())
            .setFields(fields(NAME_FIELD, String.class));

        QueryEntity second = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(AnnotatedPerson.class.getName())
            .setFields(fields(NAME_FIELD, String.class));

        ccfg.setQueryEntities(Collections.singleton(first));
        ccfg.setQueryEntities(Collections.singleton(second));

        assertEquals(1, ccfg.getQueryEntities().size());
        assertSame(second, ccfg.getQueryEntities().iterator().next());

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);

        assertEquals(AnnotatedPerson.class.getName(), entity.getValueType());
        assertEquals(Integer.class.getName(), entity.getKeyType());
        assertEquals(second.getFields(), entity.getFields());
    }

    /**
     * Verifies that a repeated {@link CacheConfiguration#setQueryEntities} call replaces the previously
     * configured entity even when both entities have the same value type.
     */
    @Test
    public void testRepeatedSetQueryEntitiesReplacesEntityWithSameValueType() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        QueryEntity first = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Person.class.getName())
            .setFields(fields(NAME_FIELD, String.class));

        QueryEntity second = new QueryEntity()
            .setKeyType(String.class.getName())
            .setValueType(Person.class.getName())
            .setFields(fields(NAME_FIELD, String.class));

        ccfg.setQueryEntities(Collections.singleton(first));
        ccfg.setQueryEntities(Collections.singleton(second));

        assertEquals(1, ccfg.getQueryEntities().size());

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);

        assertEquals(Person.class.getName(), entity.getValueType());
        assertEquals(String.class.getName(), entity.getKeyType());
        assertEquals(second.getFields(), entity.getFields());
    }

    /**
     * Verifies that a repeated {@link CacheConfiguration#setIndexedTypes} call replaces indexed types and
     * the metadata generated by the previous call.
     */
    @Test
    public void testRepeatedSetIndexedTypesReplacesPreviousConfiguration() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(FirstKey.class, Person.class);
        ccfg.setIndexedTypes(SecondKey.class, AnnotatedPerson.class);

        assertArrayEquals(
            new Class<?>[] {SecondKey.class, AnnotatedPerson.class},
            ccfg.getIndexedTypes()
        );

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);

        assertEquals(SecondKey.class.getName(), entity.getKeyType());
        assertEquals(AnnotatedPerson.class.getName(), entity.getValueType());

        CacheKeyConfiguration[] keyCfg = ccfg.getKeyConfiguration();

        assertNotNull(keyCfg);
        assertEquals(1, keyCfg.length);
        assertEquals(SecondKey.class.getName(), keyCfg[0].getTypeName());
        assertEquals("secondAffinityKey", keyCfg[0].getAffinityKeyFieldName());
    }

    /**
     * Verifies that {@link CacheConfiguration#setIndexedTypes} replaces an explicitly configured
     * key configuration.
     */
    @Test
    public void testSetIndexedTypesReplacesExplicitKeyConfiguration() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setKeyConfiguration(
            new CacheKeyConfiguration("ExplicitKey", "explicitAffinityKey")
        );

        ccfg.setIndexedTypes(SecondKey.class, Person.class);

        CacheKeyConfiguration[] keyCfg = ccfg.getKeyConfiguration();

        assertNotNull(keyCfg);
        assertEquals(1, keyCfg.length);
        assertEquals(SecondKey.class.getName(), keyCfg[0].getTypeName());
        assertEquals("secondAffinityKey", keyCfg[0].getAffinityKeyFieldName());
    }

    /**
     * Verifies that query entities cannot be configured through {@link CacheConfiguration#setQueryEntities}
     * after {@link CacheConfiguration#setIndexedTypes} was used.
     */
    @Test
    public void testSetQueryEntitiesAfterSetIndexedTypesFails() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(CACHE_NAME)
            .setIndexedTypes(Integer.class, AnnotatedPerson.class);

        String msg = String.format(MIXED_QUERY_ENTITIES_API_ERROR, CACHE_NAME);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singleton(configuredEntity(AnnotatedPerson.class))),
            CacheException.class,
            msg
        );
    }

    /**
     * Verifies that query entities cannot be configured through {@link CacheConfiguration#setIndexedTypes}
     * after {@link CacheConfiguration#setQueryEntities} was used.
     */
    @Test
    public void testSetIndexedTypesAfterSetQueryEntitiesFails() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(CACHE_NAME)
            .setQueryEntities(Collections.singleton(configuredEntity(AnnotatedPerson.class)));

        String msg = String.format(MIXED_QUERY_ENTITIES_API_ERROR, CACHE_NAME);

        assertThrows(
            log,
            () -> ccfg.setIndexedTypes(Integer.class, AnnotatedPerson.class),
            CacheException.class,
            msg
        );
    }

    /**
     * Verifies that clearing query entities does not allow switching from {@link CacheConfiguration#setIndexedTypes}
     * to {@link CacheConfiguration#setQueryEntities}.
     */
    @Test
    public void testClearQueryEntitiesDoesNotAllowSwitchFromIndexedTypes() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(CACHE_NAME)
            .setIndexedTypes(Integer.class, Person.class);

        ccfg.clearQueryEntities();

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singleton(configuredEntity(Person.class))),
            CacheException.class,
            String.format(MIXED_QUERY_ENTITIES_API_ERROR, CACHE_NAME)
        );
    }

    /**
     * Verifies that clearing query entities does not allow switching from {@link CacheConfiguration#setQueryEntities}
     * to {@link CacheConfiguration#setIndexedTypes}.
     */
    @Test
    public void testClearQueryEntitiesDoesNotAllowSwitchFromQueryEntities() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(CACHE_NAME)
            .setQueryEntities(Collections.singleton(configuredEntity(Person.class)));

        ccfg.clearQueryEntities();

        assertThrows(
            log,
            () -> ccfg.setIndexedTypes(Integer.class, Person.class),
            CacheException.class,
            String.format(MIXED_QUERY_ENTITIES_API_ERROR, CACHE_NAME)
        );
    }

    /** Verifies that clearing query entities does not prevent configuring them again through the same API. */
    @Test
    public void testSetQueryEntitiesCanBeUsedAfterClearQueryEntities() {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        QueryEntity first = configuredEntity(Person.class);
        QueryEntity second = configuredEntity(AnnotatedPerson.class);

        ccfg.setQueryEntities(Collections.singleton(first));

        ccfg.clearQueryEntities();

        ccfg.setQueryEntities(Collections.singleton(second));

        Collection<QueryEntity> entities = ccfg.getQueryEntities();

        assertEquals(1, entities.size());
        assertSame(second, entities.iterator().next());
    }

    /** Verifies that clearing query entities does not prevent configuring indexed types again through the same API. */
    @Test
    public void testSetIndexedTypesCanBeUsedAfterClearQueryEntities() {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(FirstKey.class, Person.class);

        ccfg.clearQueryEntities();

        ccfg.setIndexedTypes(SecondKey.class, AnnotatedPerson.class);

        Collection<QueryEntity> entities = ccfg.getQueryEntities();

        assertEquals(1, entities.size());

        QueryEntity entity = entities.iterator().next();

        assertEquals(SecondKey.class.getName(), entity.getKeyType());
        assertEquals(AnnotatedPerson.class.getName(), entity.getValueType());
    }

    /** Verifies that the indexed-types configuration source is preserved when {@link CacheConfiguration} is copied. */
    @Test
    public void testIndexedTypesConfigurationSourceIsPreservedOnCopy() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(CACHE_NAME)
            .setIndexedTypes(Integer.class, Person.class);

        CacheConfiguration<Integer, Person> copy = new CacheConfiguration<>(ccfg);

        assertThrows(
            log,
            () -> copy.setQueryEntities(Collections.singleton(configuredEntity(Person.class))),
            CacheException.class,
            String.format(MIXED_QUERY_ENTITIES_API_ERROR, CACHE_NAME)
        );
    }

    /** Verifies that the query-entities configuration source is preserved when {@link CacheConfiguration} is copied. */
    @Test
    public void testQueryEntitiesConfigurationSourceIsPreservedOnCopy() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(CACHE_NAME)
            .setQueryEntities(Collections.singleton(configuredEntity(Person.class)));

        CacheConfiguration<Integer, Person> copy = new CacheConfiguration<>(ccfg);

        assertThrows(
            log,
            () -> copy.setIndexedTypes(Integer.class, Person.class),
            CacheException.class,
            String.format(MIXED_QUERY_ENTITIES_API_ERROR, CACHE_NAME)
        );
    }

    /**
     * Verifies that a statically configured cache using {@link CacheConfiguration#setIndexedTypes} starts successfully
     * and preserves its query-entity configuration source during cache initialization.
     */
    @Test
    public void testCacheConfiguredWithIndexedTypesStartsSuccessfully() throws Exception {
        staticCfg = true;

        indexedTypesCfg = true;

        IgniteEx node = startGrid(0);

        assertNotNull(node.cache(CACHE_NAME));

        QueryEntity entity = singleQueryEntity(node);

        assertEquals(Person.class.getName(), entity.getValueType());
    }

    /**
     * Verifies that a node with a statically configured cache using {@link CacheConfiguration#setIndexedTypes} can
     * join an active cluster.
     */
    @Test
    public void testNodeWithIndexedTypesConfigurationJoinsCluster() throws Exception {
        staticCfg = true;
        indexedTypesCfg = true;

        IgniteEx node0 = startGrid(0);

        node0.cluster().state(ACTIVE);

        IgniteEx node1 = startGrid(1);

        assertNotNull(node1.cache(CACHE_NAME));

        QueryEntity entity = singleQueryEntity(node1);

        assertEquals(Person.class.getName(), entity.getValueType());
    }

    /**
     * Verifies that adding SQL metadata to a statically configured cache preserves {@link QueryEntityEx} and its
     * extended metadata.
     *
     * <p>A schema-add operation patches both the runtime cache configuration and the cache descriptor configuration.
     * Internal query-entity replacement must keep these configurations independent and must not modify the descriptor
     * configuration through a shared mutable collection.</p>
     */
    @Test
    public void testSchemaAddPreservesQueryEntityExMetadata() throws Exception {
        staticCfg = true;

        IgniteEx node = startGrid(0);

        node.cluster().state(ACTIVE);

        DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(CACHE_NAME);

        assertTrue(desc.cacheConfiguration().getQueryEntities().isEmpty());

        node.cache(CACHE_NAME).query(new SqlFieldsQuery(
            "CREATE TABLE TEST_TBL (" +
                "ID1 INT, " +
                "ID2 INT, " +
                "VAL VARCHAR NOT NULL, " +
                "PRIMARY KEY (ID1, ID2)" +
                ") WITH \"CACHE_NAME=" + CACHE_NAME + "\""
        )).getAll();

        Collection<QueryEntity> entities = node.context().cache()
            .cacheDescriptor(CACHE_NAME)
            .cacheConfiguration()
            .getQueryEntities();

        assertEquals(1, entities.size());

        QueryEntity entity = entities.iterator().next();

        assertTrue(entity instanceof QueryEntityEx);

        QueryEntityEx entityEx = (QueryEntityEx)entity;

        assertTrue(entityEx.sql());
        assertTrue(entityEx.isPreserveKeysOrder());
        assertTrue(entityEx.fillAbsentPKsWithDefaults());
    }

    /** */
    private static QueryEntity configuredEntity(Class<?> valCls, QueryIndex... indexes) {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put(NAME_FIELD, String.class.getName());
        fields.put(AGE_FIELD, Integer.class.getName());

        QueryEntity res = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(valCls.getName())
            .setTableName(valCls.getSimpleName())
            .setFields(fields);

        if (indexes.length != 0)
            res.setIndexes(Arrays.asList(indexes));

        return res;
    }

    /** */
    private static LinkedHashMap<String, String> fields(Object... vals) {
        assertTrue(vals.length % 2 == 0);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        for (int i = 0; i < vals.length; i += 2)
            fields.put((String)vals[i], ((Class<?>)vals[i + 1]).getName());

        return fields;
    }

    /** */
    private static QueryEntity singleQueryEntity(IgniteEx node) {
        Collection<QueryEntity> entities = (Collection<QueryEntity>)node.context().cache()
            .cacheConfiguration(CACHE_NAME)
            .getQueryEntities();

        assertEquals(1, entities.size());

        return entities.iterator().next();
    }

    /** */
    private static class Person {
        /** */
        private final String name;

        /** */
        private final int age;

        /** */
        private Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    /** */
    private static class AnnotatedPerson {
        /** */
        @QuerySqlField(index = true, notNull = true)
        private String name;

        /** */
        @QuerySqlField
        private int age;

        /** */
        @QuerySqlField
        private float weight;

        /** */
        @QuerySqlField(scale = 2)
        private float height;
    }

    /** */
    private static class FirstKey {
        /** */
        @AffinityKeyMapped
        private int firstAffinityKey;
    }

    /** */
    private static class SecondKey {
        /** */
        @AffinityKeyMapped
        private int secondAffinityKey;
    }
}
