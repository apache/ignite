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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.calcite.PseudoColumnDescriptor;
import org.apache.ignite.calcite.PseudoColumnProvider;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** For {@link PseudoColumnProvider} testing with node restart. */
public class PseudoColumnProviderWithNodeRestartTest extends GridCommonAbstractTest {
    /** */
    private static final List<PseudoColumnDescriptor> PSEUDO_COLS = new CopyOnWriteArrayList<>();

    /** */
    private boolean persistDfltDataRegion;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()
        );

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistDfltDataRegion));

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(sqlCfg)
            .setDataStorageConfiguration(dsCfg)
            .setPluginProviders(new PseudoColumnProviderTest.TestPseudoColumnPluginProvider(PSEUDO_COLS));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
        cleanPersistenceDir();

        PSEUDO_COLS.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();

        PSEUDO_COLS.clear();
    }

    /** */
    @Test
    public void testPseudoColumnWithKeyName() {
        PSEUDO_COLS.add(new PseudoColumnProviderTest.KeyToStingPseudoColumn(QueryUtils.KEY_FIELD_NAME));

        assertThrowsAnyCause(
            log,
            () -> startGrid(0),
            IgniteCheckedException.class,
            "Pseudocolumn name from plugin must not match system one: [name=_KEY"
        );
    }

    /** */
    @Test
    public void testPseudoColumnWithValName() {
        PSEUDO_COLS.add(new PseudoColumnProviderTest.KeyToStingPseudoColumn(QueryUtils.VAL_FIELD_NAME));

        assertThrowsAnyCause(
            log,
            () -> startGrid(0),
            IgniteCheckedException.class,
            "Pseudocolumn name from plugin must not match system one: [name=_VAL"
        );
    }

    /** */
    @Test
    public void testPseudoColumnsWithSameName() {
        PSEUDO_COLS.add(new PseudoColumnProviderTest.KeyToStingPseudoColumn("FOO"));
        PSEUDO_COLS.add(new PseudoColumnProviderTest.KeyToStingPseudoColumn("FOO"));

        assertThrowsAnyCause(
            log,
            () -> startGrid(0),
            IgniteCheckedException.class,
            "Pseudocolumn name from plugin must be unique: [name=FOO"
        );
    }

    /** */
    @Test
    public void testAddPseudoColumnWithSameNameAsUserCreateAfterRestart() throws Exception {
        persistDfltDataRegion = true;

        IgniteEx n = startGrid(0);
        n.cluster().state(ClusterState.ACTIVE);

        SqlFieldsQuery createTableDdl = new SqlFieldsQuery("create table PUBLIC.PERSON(id int primary key, name varchar)");
        try (FieldsQueryCursor<List<?>> c = n.context().query().querySqlFields(createTableDdl, false)) {
            c.getAll();
        }

        stopGrid(0);

        PSEUDO_COLS.add(new PseudoColumnProviderTest.KeyToStingPseudoColumn("NAME"));

        assertThrows(
            log,
            () -> startGrid(0),
            AssertionError.class,
            "Pseudocolumn name should not overlap with the user ones: " +
                "[pseudoColumnName=NAME, cacheName=SQL_PUBLIC_PERSON, schemaName=PUBLIC, tableName=PERSON]"
        );
    }
}
