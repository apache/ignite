/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.jdbc.JdbcSQLException;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
public class QueryEntityAliasesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return getConfiguration(getTestIgniteInstanceIndex(igniteInstanceName), getTestQueryEntity());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testSqlEscapeFlagMismatch() throws Exception {
        startGrid(0);

        assertThrowsAnyCause(
            log,
            () -> {
                startGrid(getConfiguration(1, getTestQueryEntity().addQueryField("salary", Integer.class.getName(), null), true));

                return null;
            },
            IgniteSpiException.class,
            "Failed to join node to the cluster, configuration conflict for cache 'default': \"isSqlEscapeAll\"" +
                " configuration property is different [local=false, remote=true]"
        );

        checkTableColumns("_KEY", "_VAL", "ID", "AGE");
    }

    /** */
    @Test
    public void testQueryEntityAliasesValidation() throws Exception {
        startGrid(0);

        assertThrowsAnyCause(
            log,
            () -> {
                startGrid(getConfiguration(1, getTestQueryEntity().addQueryField("salary", Integer.class.getName(), "AGE")));

                return null;
            },
            IgniteException.class,
            "Multiple query fields are associated with the same alias [alias=AGE]"
        );

        assertThrowsAnyCause(
            log,
            () -> {
                grid(0).createCache(new CacheConfiguration<>("test-cache")
                    .setQueryEntities(Collections.singletonList(getTestQueryEntity()
                        .addQueryField("salary", Integer.class.getName(), "AGE"))));

                return null;
            },
            IgniteException.class,
            "Multiple query fields are associated with the same alias [alias=AGE]"
        );
    }

    /** */
    @Test
    public void testQueryEntityEntityMergeAliasesConflicts() throws Exception {
        startGrid(0);

        assertThrowsAnyCause(
            log,
            () -> {
                startGrid(getConfiguration(1, new QueryEntity(String.class, Person.class)
                    .setTableName("PERSON")
                    .addQueryField("age", Integer.class.getName(), "A")));

                return null;
            },
            IgniteSpiException.class,
            "alias of age is different: local=AGE, received=A"
        );

        assertThrowsAnyCause(
            log,
            () -> {
                startGrid(getConfiguration(1, new QueryEntity(String.class, Person.class)
                    .setTableName("PERSON")
                    .addQueryField("salary", Integer.class.getName(), "AGE")));

                return null;
            },
            IgniteSpiException.class,
            "multiple fields are associated with the same alias: alias=AGE, localField=age, receivedField=salary"
        );

        checkTableColumns("_KEY", "_VAL", "ID", "AGE");

        grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("ALTER TABLE PERSON ADD salary INTEGER")).getAll();

        assertThrowsAnyCause(
            log,
            () -> {
                startGrid(getConfiguration(1, new QueryEntity(String.class, Person.class)
                    .setTableName("PERSON")
                    .addQueryField("SALARY", Integer.class.getName(), "S")));

                return null;
            },
            IgniteSpiException.class,
            "alias of SALARY is different: local=SALARY, received=S"
        );

        assertThrowsAnyCause(
            log,
            () -> {
                startGrid(getConfiguration(1, new QueryEntity(String.class, Person.class)
                    .setTableName("PERSON")
                    .addQueryField("salary", Integer.class.getName(), null)));

                return null;
            },
            IgniteSpiException.class,
            "multiple fields are associated with the same alias: alias=SALARY, localField=SALARY, receivedField=salary"
        );

        checkTableColumns("_KEY", "_VAL", "ID", "AGE", "SALARY");
    }


    /** */
    @Test
    public void testTableColumnNamesAfterSuccessfulQueryEntityMerge() throws Exception {
        startGrid(0);

        grid(0).cluster().state(INACTIVE);

        startGrid(getConfiguration(1, getTestQueryEntity().addQueryField("salary", Integer.class.getName(), null)));

        grid(0).cluster().state(ACTIVE);

        checkTableColumns("_KEY", "_VAL", "AGE", "SALARY", "ID");
    }

    /** Checks that a dynamically added column can be queried by its alias on every node. */
    @Test
    public void testDynamicColumnAlias() throws Exception {
        checkDynamicColumns(new QueryField("salary", Integer.class.getName(), "SALARY_ALIAS", true, -1, -1));
    }

    /** Checks that a dynamic update preserves the exact spelling of a quoted alias. */
    @Test
    public void testDynamicColumnMixedCaseAlias() throws Exception {
        checkDynamicColumns(new QueryField("salary", Integer.class.getName(), "salaryAlias", true, -1, -1));
    }

    /** Checks that an absent alias still falls back to the underlying field name. */
    @Test
    public void testDynamicColumnWithoutAlias() throws Exception {
        checkDynamicColumns(new QueryField("salary", Integer.class.getName(), true));
    }

    /** Checks that adding several columns preserves each alias independently. */
    @Test
    public void testDynamicColumnsAliases() throws Exception {
        checkDynamicColumns(
            new QueryField("salary", Integer.class.getName(), "SALARY_ALIAS", true, -1, -1),
            new QueryField("bonus", Integer.class.getName(), "BONUS_ALIAS", true, -1, -1)
        );
    }

    /** Checks that an aliased dynamic column can subsequently be removed using its SQL name. */
    @Test
    public void testDynamicColumnDropByAlias() throws Exception {
        checkDynamicColumns(new QueryField("salary", Integer.class.getName(), "SALARY_ALIAS", true, -1, -1));

        grid(0).context().query().dynamicColumnRemove(DEFAULT_CACHE_NAME, DEFAULT_CACHE_NAME, "PERSON",
            Collections.singletonList("SALARY_ALIAS"), false, false).get();

        for (int i = 0; i < 2; i++) {
            awaitColumnCount(i, 2);

            assertNull(typeDescriptor(i).property("SALARY_ALIAS"));

            assertEquals(Collections.singletonList(Arrays.asList(1, 42)), grid(i).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT ID, AGE FROM PERSON WHERE _KEY = ?").setArgs("person")).getAll());

            final int nodeIdx = i;

            assertThrowsAnyCause(log, () -> grid(nodeIdx).cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT SALARY_ALIAS FROM PERSON")).getAll(),
                JdbcSQLException.class, "Column \"SALARY_ALIAS\" not found");
        }
    }

    /**
     * Checks that the underlying field name does not replace the alias when removing a column.
     */
    @Test
    public void testDynamicColumnDropByFieldNameFails() throws Exception {
        checkDynamicColumns(new QueryField("salary", Integer.class.getName(), "SALARY_ALIAS", true, -1, -1));

        assertThrowsAnyCause(log, () -> grid(0).context()
                .query()
                .dynamicColumnRemove(DEFAULT_CACHE_NAME, DEFAULT_CACHE_NAME, "PERSON",
                                     Collections.singletonList("salary"), false, false)
                .get(), SchemaOperationException.class, "Column doesn't exist: salary");

        for (int i = 0; i < 2; i++) {
            assertEquals(Collections.singletonList(Collections.singletonList(100)), grid(i).cache(DEFAULT_CACHE_NAME)
                    .query(new SqlFieldsQuery("SELECT SALARY_ALIAS FROM PERSON WHERE _KEY = ?").setArgs("person"))
                    .getAll());
        }
    }

    /**
     * Adds columns to a running two-node cluster and verifies metadata and reads of existing binary data.
     *
     * @param cols Columns to add.
     * @throws Exception If failed.
     */
    private void checkDynamicColumns(QueryField... cols) throws Exception {
        startGrids(2);

        IgniteCache<String, BinaryObject> cache = grid(0).cache(DEFAULT_CACHE_NAME).withKeepBinary();

        BinaryObjectBuilder builder =
                grid(0).binary().builder(Person.class.getName()).setField("id", 1).setField("age", 42);

        for (int i = 0; i < cols.length; i++) {
            builder.setField(cols[i].name(), 100 + i);
        }

        cache.put("person", builder.build());

        grid(0).context()
                .query()
                .dynamicColumnAdd(DEFAULT_CACHE_NAME, DEFAULT_CACHE_NAME, "PERSON", Arrays.asList(cols), false, false)
                .get();

        for (int i = 0; i < 2; i++) {
            awaitColumnCount(i, 2 + cols.length);

            for (int j = 0; j < cols.length; j++) {
                String alias = cols[j].alias();

                assertNotNull("Missing dynamic property on node " + i + ": " + alias,
                              typeDescriptor(i).property(alias));

                assertEquals(Collections.singletonList(Arrays.asList(1, 42, 100 + j)), grid(i).cache(DEFAULT_CACHE_NAME)
                        .query(new SqlFieldsQuery(
                                "SELECT ID, AGE, \"" + alias + "\" FROM PERSON WHERE _KEY = ?").setArgs("person"))
                        .getAll());
            }
        }
    }

    /**
     * Adds columns to a running two-node cluster and verifies metadata and reads of existing binary data.
     */
    private void checkTableColumns(String... expCols) {
        List<?> cols = grid(0).cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select * from \"SYS\".TABLE_COLUMNS"))
            .getAll()
            .stream()
            .map(val -> (String)val.get(0))
            .collect(Collectors.toList());

        List<String> exp = Arrays.asList(expCols);

        assertEquals(exp.size(), cols.size());
        assertTrue(cols.containsAll(exp));
    }

    /** */
    private QueryEntity getTestQueryEntity() {
        return new QueryEntity(String.class, Person.class)
            .setTableName("PERSON")
            .addQueryField("id", Integer.class.getName(), "ID")
            .addQueryField("age", Integer.class.getName(), "AGE");
    }

    /** */
    private IgniteConfiguration getConfiguration(int nodeIdx, QueryEntity qryEntity) throws Exception {
        return getConfiguration(nodeIdx, qryEntity, false);
    }

    /** */
    private IgniteConfiguration getConfiguration(int nodeIdx, QueryEntity qryEntity, boolean isSqlEscapeAll) throws Exception {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setSqlEscapeAll(isSqlEscapeAll)
            .setQueryEntities(Collections.singletonList(qryEntity));

        return super.getConfiguration(getTestIgniteInstanceName(nodeIdx)).setCacheConfiguration(ccfg);
    }

    /**
     * Waits for a schema operation to reach a node without relying on a fixed delay.
     *
     * @param nodeIdx Node index.
     * @param expCnt Expected count of user columns.
     * @throws Exception If failed.
     */
    private void awaitColumnCount(int nodeIdx, int expCnt) throws Exception {
        assertTrue("Schema update did not reach node " + nodeIdx,
                   GridTestUtils.waitForCondition(() -> typeDescriptor(nodeIdx).fields().size() == expCnt,
                                                  getTestTimeout()));
    }

    /**
     * @param nodeIdx Node index.
     * @return Descriptor of the test table.
     */
    private GridQueryTypeDescriptor typeDescriptor(int nodeIdx) {
        return grid(nodeIdx).context().query().types(DEFAULT_CACHE_NAME).iterator().next();
    }

    /** */
    class Person {
        /** */
        private int id;

        /** */
        private int age;

        /** */
        private int salary;
    }
}
