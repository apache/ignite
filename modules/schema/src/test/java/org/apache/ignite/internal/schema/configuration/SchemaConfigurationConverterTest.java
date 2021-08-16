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

package org.apache.ignite.internal.schema.configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.SortedIndexColumn;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.builder.HashIndexBuilder;
import org.apache.ignite.schema.builder.PartialIndexBuilder;
import org.apache.ignite.schema.builder.PrimaryIndexBuilder;
import org.apache.ignite.schema.builder.SchemaTableBuilder;
import org.apache.ignite.schema.builder.SortedIndexBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * SchemaConfigurationConverter tests.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class SchemaConfigurationConverterTest {
    /** Table builder. */
    private SchemaTableBuilder tblBuilder;

    /** Configuration registry with one table for each test. */
    private ConfigurationRegistry confRegistry;

    /**
     * Prepare configuration registry for test.
     *
     * @throws ExecutionException If failed.
     * @throws InterruptedException If failed.
     */
    @BeforeEach
    public void createRegistry() throws ExecutionException, InterruptedException {
        confRegistry = new ConfigurationRegistry(
            List.of(TablesConfiguration.KEY),
            Map.of(TableValidator.class, Set.of(SchemaTableValidatorImpl.INSTANCE)),
            new TestConfigurationStorage(DISTRIBUTED)
        );

        confRegistry.start();

        tblBuilder = SchemaBuilders.tableBuilder("SNAME", "TNAME")
            .columns(
                SchemaBuilders.column("COL1", ColumnType.DOUBLE).build(),
                SchemaBuilders.column("COL2", ColumnType.DOUBLE).build(),
                SchemaBuilders.column("A", ColumnType.INT8).build(),
                SchemaBuilders.column("B", ColumnType.INT8).build(),
                SchemaBuilders.column("C", ColumnType.INT8).build()
            ).withPrimaryKey("COL1");

        SchemaTable tbl = tblBuilder.build();

        confRegistry.getConfiguration(TablesConfiguration.KEY).change(
            ch -> {
                SchemaConfigurationConverter.createTable(tbl, ch)
                    .changeTables(tblsCh -> tblsCh.createOrUpdate(tbl.canonicalName(),
                        tblCh -> tblCh.changeReplicas(1)));
            }).get();
    }

    @AfterEach
    void tearDown() {
        confRegistry.stop();
    }

    /**
     * Add/remove HashIndex into configuration and read it back.
     */
    @Test
    public void testConvertHashIndex() throws ExecutionException, InterruptedException {
        HashIndexBuilder builder = SchemaBuilders.hashIndex("testHI")
            .withColumns("A", "B", "C")
            .withHints(Collections.singletonMap("param", "value"));
        HashIndex idx = builder.build();

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        SchemaTable tbl = SchemaConfigurationConverter.convert(getTbl().value());

        HashIndex idx2 = (HashIndex)getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("HASH", idx2.type());
        assertEquals(3, idx2.columns().size());
    }

    /**
     * Add/remove SortedIndex into configuration and read it back.
     */
    @Test
    public void testConvertSortedIndex() throws ExecutionException, InterruptedException {
        SortedIndexBuilder builder = SchemaBuilders.sortedIndex("SIDX");

        builder.addIndexColumn("A").asc().done();
        builder.addIndexColumn("B").desc().done();

        builder.unique();

        SortedIndex idx = builder.build();

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        SchemaTable tbl = SchemaConfigurationConverter.convert(getTbl().value());

        SortedIndex idx2 = (SortedIndex)getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("SORTED", idx2.type());
        assertEquals(2, idx2.columns().size());
    }

    /**
     * Add/remove PrimaryIndex into configuration and read it back.
     */
    @Test
    public void testPKIndex() throws ExecutionException, InterruptedException {
        PrimaryIndexBuilder builder = SchemaBuilders.pkIndex();
        builder.addIndexColumn("COL1").desc().done();
        builder.addIndexColumn("A").desc().done();
        builder.addIndexColumn("B").asc().done();
        builder.withAffinityColumns("COL1");

        PrimaryIndex idx = builder.build();

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        SchemaTable tbl = SchemaConfigurationConverter.convert(getTbl().value());

        PrimaryIndex idx2 = (PrimaryIndex)getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("PK", idx2.type());
        assertEquals(idx.columns().stream().map(SortedIndexColumn::name).collect(Collectors.toList()),
            idx2.columns().stream().map(SortedIndexColumn::name).collect(Collectors.toList()));
        assertEquals(idx.affinityColumns(), idx2.affinityColumns());
    }

    /**
     * Add/remove PartialIndex into configuration and read it back.
     */
    @Test
    public void testPartialIndex() throws ExecutionException, InterruptedException {
        PartialIndexBuilder builder = SchemaBuilders.partialIndex("TEST");

        builder.addIndexColumn("A").done();
        builder.withExpression("WHERE A > 0");

        PartialIndex idx = builder.build();

        getTbl().change(ch -> SchemaConfigurationConverter.addIndex(idx, ch)).get();

        SchemaTable tbl = SchemaConfigurationConverter.convert(getTbl().value());

        PartialIndex idx2 = (PartialIndex) getIdx(idx.name(), tbl.indices());

        assertNotNull(idx2);
        assertEquals("PARTIAL", idx2.type());
        assertEquals(idx.columns().size(), idx2.columns().size());
    }

    /**
     * Add/remove table and read it back.
     */
    @Test
    public void testConvertTable() {
        SchemaTable tbl = tblBuilder.build();

        TableConfiguration tblCfg = confRegistry.getConfiguration(TablesConfiguration.KEY).tables()
            .get(tbl.canonicalName());

        SchemaTable tbl2 = SchemaConfigurationConverter.convert(tblCfg);

        assertEquals(tbl.canonicalName(), tbl2.canonicalName());
        assertEquals(tbl.indices().size(), tbl2.indices().size());
        assertEquals(tbl.keyColumns().size(), tbl2.keyColumns().size());
        assertEquals(tbl.affinityColumns().size(), tbl2.affinityColumns().size());
        assertEquals(tbl.valueColumns().size(), tbl2.valueColumns().size());
    }

    /**
     * Get tests default table configuration.
     *
     * @return Configuration of default table.
     */
    private TableConfiguration getTbl() {
        return confRegistry.getConfiguration(TablesConfiguration.KEY).tables().get(tblBuilder.build().canonicalName());
    }

    /**
     * Get table index by name.
     *
     * @param name Index name to find.
     * @param idxs Table indexes.
     * @return Index or {@code null} if there are no index with such name.
     */
    private TableIndex getIdx(String name, Collection<TableIndex> idxs) {
        return idxs.stream().filter(idx -> name.equals(idx.name())).findAny().orElse(null);
    }
}
