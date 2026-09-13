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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import javax.cache.CacheException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.sql.SqlIndexView;
import org.apache.ignite.spi.systemview.view.sql.SqlTableColumnView;
import org.apache.ignite.spi.systemview.view.sql.SqlTableView;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_TBLS_VIEW;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_TBL_COLS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Tests for merging QueryEntity metadata in CacheConfiguration. */
public class CacheConfigurationQueryEntityMergeTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "query-entity-merge-cache";

    /** */
    private static final String COMPOSITE_IDX = "PERSON_NAME_AGE_IDX";

    /** */
    private static final String NAME_IDX = "EXPLICIT_NAME_IDX";

    /** */
    private static final String AGE_IDX = "EXPLICIT_AGE_IDX";

    /** */
    private static final String NAME_FIELD = "name";

    /** */
    private static final String AGE_FIELD = "age";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** Query entities with different value types must not be merged. */
    @Test
    public void testDifferentValueTypesAreNotMerged() throws Exception {
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

        node.createCache(ccfg);

        Collection<QueryEntity> entities = entities(node);

        assertEquals(2, entities.size());

        for (Class<?> cls : List.of(Person.class, AnnotatedPerson.class))
            assertTrue(entities.stream().anyMatch(e -> cls.getName().equals(e.getValueType())));
    }

    /** Query entities with the same value type but different key type are a conflict. */
    @Test
    public void testConflictingKeyTypesFail() {
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

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
            "cacheName=%s, property=keyType, existingValue=%s, incomingValue=%s]",
            CACHE_NAME, Integer.class.getName(), String.class.getName());

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singleton(second)),
            CacheException.class,
            msg
        );
    }

    /**
     * Checks that query entities with conflicting effective key types cannot be merged.
     * <p>
     * The first entity does not define {@code keyType} explicitly. Instead, its effective key type is derived from
     * {@code keyFieldName} and the corresponding field type. The second entity defines a different key type explicitly.
     * <p>
     * Although {@link QueryEntity#getKeyType()} returns {@code null} for the first entity,
     * {@link QueryEntity#findKeyType()} resolves its key type from the field metadata. Therefore, the entities must be
     * treated as having conflicting key types.
     */
    @Test
    public void testConflictingImplicitAndExplicitKeyTypes() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        QueryEntity first = new QueryEntity()
            .setValueType(Person.class.getName())
            .setFields(fields("id", Integer.class))
            .setKeyFieldName("id"); // keyType is null

        QueryEntity second = new QueryEntity()
            .setValueType(Person.class.getName())
            .setKeyType(String.class.getName());

        ccfg.setQueryEntities(Collections.singleton(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=keyType, existingValue=%s, incomingValue=%s]",
            CACHE_NAME, Integer.class.getName(), String.class.getName());

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singleton(second)),
            CacheException.class,
            msg
        );
    }

    /**
     * Checks that query entities with matching effective value types can be merged when one value type is defined
     * implicitly and the other one explicitly.
     * <p>
     * The first entity does not define {@code valueType} explicitly. Its effective value type is derived from
     * {@code valueFieldName} and the corresponding field type, so {@link QueryEntity#getValueType()} returns
     * {@code null}, while {@link QueryEntity#findValueType()} resolves it to the person type.
     * <p>
     * The second entity defines the same value type explicitly. Since both entities have the same effective value
     * type, they must be merged without a conflict.
     */
    @Test
    public void testMatchingImplicitAndExplicitValueTypesAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        QueryEntity first = new QueryEntity()
            .setFields(fields("val", Person.class))
            .setValueFieldName("val"); // valueType is null

        QueryEntity second = new QueryEntity().setValueType(Person.class.getName());

        ccfg.setQueryEntities(Collections.singleton(first));
        ccfg.setQueryEntities(Collections.singleton(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals("val", entity.getValueFieldName());
        assertEquals(Person.class.getName(), entity.getFields().get("val"));
        assertEquals(Person.class.getName(), entity.getValueType());
    }

    /** A missing table name in the first entity must be filled from the second entity. */
    @Test
    public void testTableNameIsFilledFromSecondEntity() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String tblName = "PERSON";

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        first.setTableName(null);

        QueryEntity second = personEntity(fields(AGE_FIELD, Integer.class));
        second.setTableName(tblName);

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        SqlTableView tbl = cacheTable(node);
        assertNotNull(tbl);

        assertEquals(tblName, tbl.tableName());
    }

    /** Same table names for one query entity are merged. */
    @Test
    public void testSameTableNamesAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String tblName = "PERSON";

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        first.setTableName(tblName);

        QueryEntity second = personEntity(fields(AGE_FIELD, Integer.class));
        second.setTableName(tblName);

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        SqlTableView tbl = cacheTable(node);
        assertNotNull(tbl);

        assertEquals(tblName, tbl.tableName());
    }

    /** Different configured table names are a conflict. */
    @Test
    public void testConflictingTableNamesFails() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String tableA = "TABLE_A";
        String tableB = "TABLE_B";

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        first.setTableName(tableA);

        QueryEntity second = personEntity(fields(AGE_FIELD, Integer.class));
        second.setTableName(tableB);

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=tableName, existingValue=%s, incomingValue=%s]",
            CACHE_NAME, tableA, tableB);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** A missing key field name in the first entity must be filled from the second entity. */
    @Test
    public void testKeyFieldNameIsFilledFromSecondEntity() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String keyFieldName = "key";

        QueryEntity first = personEntity(fields(keyFieldName, Integer.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(keyFieldName, Integer.class, NAME_FIELD, String.class));

        second.setKeyFieldName(keyFieldName);

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(keyFieldName, entity.getKeyFieldName());
    }

    /** Same key field names must be merged. */
    @Test
    public void testSameKeyFieldNameIsMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String keyFieldName = "key";

        QueryEntity first = personEntity(fields(keyFieldName, Integer.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(keyFieldName, Integer.class, NAME_FIELD, String.class));

        first.setKeyFieldName(keyFieldName);
        second.setKeyFieldName(keyFieldName);

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(keyFieldName, entity.getKeyFieldName());
    }

    /** Different key field names for the same query entity are a conflict. */
    @Test
    public void testConflictingKeyFieldNamesFail() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String key1 = "key1";
        String key2 = "key2";

        QueryEntity first = personEntity(fields(key1, Integer.class, key2, Integer.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(key1, Integer.class, key2, Integer.class, NAME_FIELD, String.class));

        first.setKeyFieldName(key1);
        second.setKeyFieldName(key2);

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=keyFieldName, existingValue=%s, incomingValue=%s]",
            CACHE_NAME, key1, key2);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** A missing value field name in the first entity must be filled from the second entity. */
    @Test
    public void testValueFieldNameIsFilledFromSecondEntity() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String valFieldName = "value";

        QueryEntity first = personEntity(fields(valFieldName, Person.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(valFieldName, Person.class, NAME_FIELD, String.class));

        second.setValueFieldName(valFieldName);

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(valFieldName, entity.getValueFieldName());
    }

    /** Same value field names must be merged. */
    @Test
    public void testSameValueFieldNameIsMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String valFieldName = "value";

        QueryEntity first = personEntity(fields(valFieldName, Integer.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(valFieldName, Integer.class, NAME_FIELD, String.class));

        first.setValueFieldName(valFieldName);
        second.setValueFieldName(valFieldName);

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(valFieldName, entity.getValueFieldName());
    }

    /** Different value field names for the same query entity are a conflict. */
    @Test
    public void testConflictingValueFieldNamesFail() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String val1 = "value1";
        String val2 = "value2";

        QueryEntity first = personEntity(fields(val1, Person.class, val2, Person.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(val1, Person.class, val2, Person.class, NAME_FIELD, String.class));

        first.setValueFieldName(val1);
        second.setValueFieldName(val2);

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=valueFieldName, existingValue=%s, incomingValue=%s]",
            CACHE_NAME, val1, val2);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** Same field with the same type must be merged. */
    @Test
    public void testSameFieldWithSameTypeIsMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setQueryEntities(Collections.singletonList(personEntity(fields(NAME_FIELD, String.class))));
        ccfg.setQueryEntities(Collections.singletonList(personEntity(fields(NAME_FIELD, String.class))));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(
            Map.of(NAME_FIELD, String.class.getName()),
            entity.getFields()
        );
    }

    /** Same field with different types is a conflict. */
    @Test
    public void testSameFieldWithDifferentTypesFails() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(NAME_FIELD, Integer.class));

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata " +
                "[cacheName=%s, property=fieldType[%s], existingValue=%s, incomingValue=%s]",
            CACHE_NAME, NAME_FIELD, String.class.getName(), Integer.class.getName());

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** Different key fields for the same entity must be merged. */
    @Test
    public void testDifferentKeyFieldsAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Object, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String key1 = "id";
        String key2 = "otherId";

        QueryEntity first = personEntity(fields(key1, Integer.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(key2, Integer.class, NAME_FIELD, String.class));

        first.setKeyFields(Collections.singleton(key1));
        second.setKeyFields(Collections.singleton(key2));

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(new LinkedHashSet<>(Arrays.asList(key1, key2)), entity.getKeyFields());
    }

    /** Same key fields for the same entity must be merged. */
    @Test
    public void testSameKeyFieldsAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Object, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String key = "id";

        QueryEntity first = personEntity(fields(key, Integer.class, NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(key, Integer.class, NAME_FIELD, String.class));

        first.setKeyFields(Collections.singleton(key));
        second.setKeyFields(Collections.singleton(key));

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(Collections.singleton("id"), entity.getKeyFields());
    }

    /** NOT NULL fields from annotation and explicit configuration must be merged. */
    @Test
    public void testNotNullFieldsAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, AnnotatedPerson> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, AnnotatedPerson.class);

        ccfg.setQueryEntities(Collections.singletonList(
            configuredEntity(AnnotatedPerson.class)
                .setNotNullFields(Collections.singleton(AGE_FIELD))
        ));

        node.createCache(ccfg);

        List<SqlTableColumnView> cols = cacheColumns(node);

        SqlTableColumnView nameCol = findColumn(cols, NAME_FIELD);
        SqlTableColumnView ageCol = findColumn(cols, AGE_FIELD);

        assertNotNull(nameCol);
        assertNotNull(ageCol);

        assertFalse(nameCol.nullable());
        assertFalse(ageCol.nullable());
    }

    /** Same NOT NULL fields from annotation and explicit configuration must be merged. */
    @Test
    public void testSameNotNullFieldsAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, AnnotatedPerson> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, AnnotatedPerson.class);

        ccfg.setQueryEntities(Collections.singletonList(
            configuredEntity(AnnotatedPerson.class)
                .setNotNullFields(Collections.singleton(NAME_FIELD))
        ));

        node.createCache(ccfg);

        List<SqlTableColumnView> cols = cacheColumns(node);

        SqlTableColumnView nameCol = findColumn(cols, NAME_FIELD);
        assertNotNull(nameCol);

        assertFalse(nameCol.nullable());
    }

    /** Aliases for different entity fields must be merged. */
    @Test
    public void testAliasesForDifferentFieldsAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String nameAlias = "name-alias";
        String ageAlias = "age-alias";

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(AGE_FIELD, Integer.class));

        first.setAliases(Collections.singletonMap(NAME_FIELD, nameAlias));
        second.setAliases(Collections.singletonMap(AGE_FIELD, ageAlias));

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(2, entity.getAliases().size());

        String actualNameAlias = entity.getAliases().get(NAME_FIELD);

        assertNotNull(actualNameAlias);
        assertTrue(nameAlias.equalsIgnoreCase(actualNameAlias));

        String actualAgeAlias = entity.getAliases().get(AGE_FIELD);

        assertNotNull(actualAgeAlias);
        assertTrue(ageAlias.equalsIgnoreCase(actualAgeAlias));
    }

    /** Different aliases for the same field are a conflict. */
    @Test
    public void testDifferentAliasesForSameFieldFail() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String alias1 = "alias1";
        String alias2 = "alias2";

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(NAME_FIELD, String.class));

        first.setAliases(Collections.singletonMap(NAME_FIELD, alias1));
        second.setAliases(Collections.singletonMap(NAME_FIELD, alias2));

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=aliases[%s], existingValue=%s, incomingValue=%s]",
            CACHE_NAME, NAME_FIELD, alias1, alias2);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** Default field values for different fields must be merged. */
    @Test
    public void testDefaultFieldValuesAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String defName = "John Doe";
        int defAge = 30;

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(AGE_FIELD, Integer.class));

        first.setDefaultFieldValues(Collections.singletonMap(NAME_FIELD, defName));
        second.setDefaultFieldValues(Collections.singletonMap(AGE_FIELD, defAge));

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(2, entity.getDefaultFieldValues().size());

        String actualDefName = (String)entity.getDefaultFieldValues().get(NAME_FIELD);
        assertNotNull(actualDefName);

        assertEquals(defName, actualDefName);

        int actualDefAge = (int)entity.getDefaultFieldValues().get(AGE_FIELD);

        assertEquals(defAge, actualDefAge);
    }

    /** Different aliases for the same field are a conflict. */
    @Test
    public void testDifferentDefaultValuesForSameFieldFail() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String defName1 = "NAME_A";
        String defName2 = "NAME_B";

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(NAME_FIELD, String.class));

        first.setDefaultFieldValues(Collections.singletonMap(NAME_FIELD, defName1));
        second.setDefaultFieldValues(Collections.singletonMap(NAME_FIELD, defName2));

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=defaultFieldValues[%s], existingValue=%s, incomingValue=%s]",
            CACHE_NAME, NAME_FIELD, defName1, defName2);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** A missing precision definition in the first entity must be filled from the second entity. */
    @Test
    public void testPrecisionIsMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        int precision = 50;

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(NAME_FIELD, String.class));

        second.setFieldsPrecision(Collections.singletonMap(NAME_FIELD, precision));

        ccfg.setQueryEntities(Collections.singletonList(first));
        ccfg.setQueryEntities(Collections.singletonList(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(
            Map.of(NAME_FIELD, precision),
            entity.getFieldsPrecision()
        );
    }

    /** Equal precision definitions are merged. */
    @Test
    public void testSamePrecisionIsMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        int precision = 50;

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(NAME_FIELD, String.class));

        first.setFieldsPrecision(Collections.singletonMap(NAME_FIELD, precision));
        second.setFieldsPrecision(Collections.singletonMap(NAME_FIELD, precision));

        ccfg.setQueryEntities(Collections.singleton(first));
        ccfg.setQueryEntities(Collections.singleton(second));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        assertEquals(
            Map.of(NAME_FIELD, precision),
            entity.getFieldsPrecision()
        );
    }

    /** Different precision definitions for the same field are a conflict. */
    @Test
    public void testDifferentPrecisionDefinitionsFail() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        int precision1 = 50;
        int precision2 = 100;

        QueryEntity first = personEntity(fields(NAME_FIELD, String.class));
        QueryEntity second = personEntity(fields(NAME_FIELD, String.class));

        first.setFieldsPrecision(Collections.singletonMap(NAME_FIELD, precision1));
        second.setFieldsPrecision(Collections.singletonMap(NAME_FIELD, precision2));

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=fieldsPrecision[%s], existingValue=%s, incomingValue=%s]",
            CACHE_NAME, NAME_FIELD, precision1, precision2);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** Scale definitions for different entity fields are merged. */
    @Test
    public void testScaleDefinitionsForDifferentFieldsAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, AnnotatedPerson> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, AnnotatedPerson.class);

        ccfg.setQueryEntities(Collections.singletonList(
            configuredEntity(AnnotatedPerson.class)
                .setFieldsScale(Collections.singletonMap("weight", 2))
        ));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);
        assertNotNull(entity);

        int weightScale = entity.getFieldsScale().get("weight");
        assertEquals(2, weightScale);

        int heightScale = entity.getFieldsScale().get("height");
        assertEquals(2, heightScale);
    }

    /** Different scale definitions for the same entity field are a conflict. */
    @Test
    public void testConflictingScaleDefinitionsFail() {
        CacheConfiguration<Integer, AnnotatedPerson> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, AnnotatedPerson.class);

        String heightField = "height";
        int heightScale = 3;

        QueryEntity configured = configuredEntity(AnnotatedPerson.class)
            .setFieldsScale(Collections.singletonMap(heightField, heightScale));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
            "cacheName=%s, property=fieldsScale[%s], existingValue=2, incomingValue=%s]",
            CACHE_NAME, heightField, heightScale);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(configured)),
            CacheException.class,
            msg
        );
    }

    /** Configured indexedTypes must not prevent configured composite index from being created. */
    @Test
    public void testCompositeIndexIsCreatedWithIndexedTypes() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<Integer, Person>(CACHE_NAME)
            .setIndexedTypes(Integer.class, Person.class)
            .setQueryEntities(Collections.singletonList(
                configuredEntity(Person.class, index(COMPOSITE_IDX, NAME_FIELD, AGE_FIELD))
            ));

        node.createCache(ccfg);

        List<SqlIndexView> indexes = cacheIndexes(node);

        assertTrue(hasIndex(indexes, COMPOSITE_IDX));

        assertEquals(1, indexes.stream().filter(idx -> !idx.isPk()).count());
    }

    /** Annotation index and configured composite index must coexist. */
    @Test
    public void testAnnotationAndConfiguredIndexesAreCreated() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, AnnotatedPerson> ccfg = new CacheConfiguration<Integer, AnnotatedPerson>(CACHE_NAME)
            .setIndexedTypes(Integer.class, AnnotatedPerson.class)
            .setQueryEntities(Collections.singletonList(
                configuredEntity(AnnotatedPerson.class, index(COMPOSITE_IDX, NAME_FIELD, AGE_FIELD))
            ));

        node.createCache(ccfg);

        List<SqlIndexView> indexes = cacheIndexes(node);

        String annotationIdxName = annotationIndexName();

        assertTrue(hasIndex(indexes, annotationIdxName));
        assertTrue(hasIndex(indexes, COMPOSITE_IDX));

        assertEquals(2, indexes.stream().filter(idx -> !idx.isPk()).count());
    }

    /** Reverse order must work as well: queryEntities first, indexedTypes second. */
    @Test
    public void testReverseConfigurationOrder() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, AnnotatedPerson> ccfg = new CacheConfiguration<Integer, AnnotatedPerson>(CACHE_NAME)
            .setQueryEntities(Collections.singletonList(
                configuredEntity(AnnotatedPerson.class, index(COMPOSITE_IDX, NAME_FIELD, AGE_FIELD))
            ))
            .setIndexedTypes(Integer.class, AnnotatedPerson.class);

        node.createCache(ccfg);

        List<SqlIndexView> indexes = cacheIndexes(node);

        assertTrue(hasIndex(indexes, annotationIndexName()));
        assertTrue(hasIndex(indexes, COMPOSITE_IDX));

        assertEquals(2, indexes.stream().filter(idx -> !idx.isPk()).count());
    }

    /** Metadata added by several consecutive setQueryEntities calls must accumulate. */
    @Test
    public void testSeveralSetQueryEntitiesCreateAllIndexes() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setQueryEntities(Collections.singletonList(
            configuredEntity(Person.class, index(NAME_IDX, NAME_FIELD))
        ));

        ccfg.setQueryEntities(Collections.singletonList(
            configuredEntity(Person.class, index(AGE_IDX, AGE_FIELD))
        ));

        ccfg.setQueryEntities(Collections.singletonList(
            configuredEntity(Person.class, index(COMPOSITE_IDX, NAME_FIELD, AGE_FIELD))
        ));

        node.createCache(ccfg);

        List<SqlIndexView> indexes = cacheIndexes(node);

        assertTrue(hasIndex(indexes, NAME_IDX));
        assertTrue(hasIndex(indexes, AGE_IDX));
        assertTrue(hasIndex(indexes, COMPOSITE_IDX));

        assertEquals(3, indexes.stream().filter(idx -> !idx.isPk()).count());
    }

    /** Identical index definitions must be deduplicated. */
    @Test
    public void testSameIndexDefinitionIsDeduplicated() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String idxName = "DUP_IDX";

        QueryIndex idx = index(idxName, NAME_FIELD);

        ccfg.setQueryEntities(Collections.singletonList(personEntity(fields(NAME_FIELD, String.class), idx)));
        ccfg.setQueryEntities(Collections.singletonList(personEntity(fields(NAME_FIELD, String.class), idx)));

        node.createCache(ccfg);

        List<SqlIndexView> indexes = cacheIndexes(node);

        assertTrue(hasIndex(indexes, idxName));

        assertEquals(1, indexes.stream().filter(i -> !i.isPk()).count());
    }

    /** Same index name with different definition is a conflict. */
    @Test
    public void testSameIndexNameWithDifferentDefinitionFails() {
        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        String ageField = AGE_FIELD;
        String conflictIdx = "CONFLICT_IDX";

        QueryEntity first = configuredEntity(Person.class, index(conflictIdx, NAME_FIELD));
        QueryEntity second = configuredEntity(Person.class, index(conflictIdx, ageField));

        ccfg.setQueryEntities(Collections.singletonList(first));

        String msg = String.format("Failed to merge query entities due to conflicting metadata [" +
                "cacheName=%s, property=index[%s], " +
                "existingValue=QueryIndex [name=%s, fields=LinkedHashMap {%s=true}, type=SORTED, inlineSize=-1], " +
                "incomingValue=QueryIndex [name=%s, fields=LinkedHashMap {%s=true}, type=SORTED, inlineSize=-1]]",
            CACHE_NAME, conflictIdx, conflictIdx, NAME_FIELD, conflictIdx, ageField);

        assertThrows(
            log,
            () -> ccfg.setQueryEntities(Collections.singletonList(second)),
            CacheException.class,
            msg
        );
    }

    /** Indexes without explicitly configured names are merged correctly. */
    @Test
    public void testIndexesWithoutExplicitNamesAreMerged() throws Exception {
        IgniteEx node = startGrid(0);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);

        QueryIndex firstIdx = new QueryIndex(Collections.singletonList(NAME_FIELD), QueryIndexType.SORTED);
        assertNull(firstIdx.getName());

        QueryIndex secondIdx = new QueryIndex(Arrays.asList(NAME_FIELD, AGE_FIELD), QueryIndexType.SORTED);
        assertNull(secondIdx.getName());

        QueryIndex thirdIdx = index(AGE_IDX, AGE_FIELD);
        assertNotNull(thirdIdx.getName());

        ccfg.setQueryEntities(Collections.singletonList(configuredEntity(Person.class, firstIdx)));
        ccfg.setQueryEntities(Collections.singletonList(configuredEntity(Person.class, secondIdx)));
        ccfg.setQueryEntities(Collections.singletonList(configuredEntity(Person.class, thirdIdx)));

        node.createCache(ccfg);

        QueryEntity entity = singleQueryEntity(node);

        assertEquals(3, entity.getIndexes().size());

        for (QueryIndex idx : List.of(firstIdx, secondIdx, thirdIdx))
            assertTrue(entity.getIndexes().stream().anyMatch(i -> i.getFields().equals(idx.getFields())));
    }

    /** */
    private static QueryEntity personEntity(LinkedHashMap<String, String> fields, QueryIndex... indexes) {
        QueryEntity entity = new QueryEntity()
            .setKeyType(Integer.class.getName())
            .setValueType(Person.class.getName())
            .setTableName(Person.class.getSimpleName())
            .setFields(fields);

        if (indexes.length != 0)
            entity.setIndexes(Arrays.asList(indexes));

        return entity;
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
    private static QueryIndex index(String name, String... fields) {
        return new QueryIndex(Arrays.asList(fields), QueryIndexType.SORTED).setName(name);
    }

    /** */
    private static List<SqlIndexView> cacheIndexes(IgniteEx node) {
        SystemView<SqlIndexView> indexes = node.context().systemView().view("indexes");
        assertNotNull(indexes);

        List<SqlIndexView> res = new ArrayList<>();

        for (SqlIndexView idx : indexes) {
            if (CACHE_NAME.equals(idx.cacheName()))
                res.add(idx);
        }

        return res;
    }

    /** */
    private static boolean hasIndex(List<SqlIndexView> indexes, String idxName) {
        return indexes.stream().anyMatch(idx -> idxName.equalsIgnoreCase(idx.indexName()));
    }

    /** */
    private static String annotationIndexName() {
        QueryEntity entity = new QueryEntity(Integer.class, AnnotatedPerson.class);

        QueryIndex idx = entity.getIndexes().iterator().next();

        return QueryUtils.indexName(entity, idx);
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
    private static SqlTableView cacheTable(IgniteEx node) {
        SystemView<SqlTableView> tables = node.context().systemView().view(SQL_TBLS_VIEW);
        assertNotNull(tables);

        SqlTableView res = null;

        for (SqlTableView tbl : tables) {
            if (CACHE_NAME.equals(tbl.cacheName()))
                res = tbl;
        }

        return res;
    }

    /** */
    private static List<SqlTableColumnView> cacheColumns(IgniteEx node) {
        SystemView<SqlTableColumnView> cols = node.context().systemView().view(SQL_TBL_COLS_VIEW);
        assertNotNull(cols);

        List<SqlTableColumnView> res = new ArrayList<>();

        SqlTableView tbl = cacheTable(node);
        assertNotNull(tbl);

        for (SqlTableColumnView col : cols) {
            if (tbl.tableName().equals(col.tableName()))
                res.add(col);
        }

        return res;
    }

    /** */
    private static SqlTableColumnView findColumn(List<SqlTableColumnView> cols, String colName) {
        SqlTableColumnView res = null;

        for (SqlTableColumnView col : cols) {
            if (col.columnName().equalsIgnoreCase(colName))
                res = col;
        }

        return res;
    }

    /** */
    private static Collection<QueryEntity> entities(IgniteEx node) {
        return (Collection<QueryEntity>)node.context().cache().cacheConfiguration(CACHE_NAME).getQueryEntities();
    }

    /** */
    private static QueryEntity singleQueryEntity(IgniteEx node) {
        Collection<QueryEntity> entities = entities(node);

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
}
