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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageView;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.PartialIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

/**
 * TableValidatorImplTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
@ExtendWith(ConfigurationExtension.class)
public class TableValidatorImplTest {
    /** Basic table configuration to mutate and then validate. */
    @InjectConfiguration(
            value = "mock.tables.table {\n"
            + "    name = schema.table,\n"
            + "    columns.id {name = id, type.type = STRING, nullable = true},\n"
            + "    primaryKey {columns = [id], affinityColumns = [id]},\n"
            + "    indices.foo {type = HASH, name = foo, colNames = [id]}"
            + "}",
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class, SortedIndexConfigurationSchema.class, PartialIndexConfigurationSchema.class
            }
    )
    private TablesConfiguration tablesCfg;

    /** Tests that validator finds no issues in a simple valid configuration. */
    @Test
    public void testNoIssues(
            @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class) DataStorageConfiguration dbCfg
    ) {
        ValidationContext<NamedListView<TableView>> ctx = mockContext(null, dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = validate(ctx);

        assertThat(issuesCaptor.getAllValues(), is(empty()));
    }

    /** Tests that the validator catches nonexistent data regions. */
    @Test
    public void testMissingDataRegion(
            @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class) DataStorageConfiguration dbCfg
    ) throws Exception {
        tablesCfg.tables().get("table").dataRegion().update("r0").get(1, TimeUnit.SECONDS);

        ValidationContext<NamedListView<TableView>> ctx = mockContext(null, dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = validate(ctx);

        assertEquals(1, issuesCaptor.getAllValues().size());

        assertEquals(
                "Data region 'r0' configured for table 'schema.table' isn't found",
                issuesCaptor.getValue().message()
        );
    }

    /** Tests that new data region must have the same type. */
    @Test
    public void testChangeDataRegionType(
            @InjectConfiguration(
                    value = "mock.regions.r0.type = pagemem",
                    polymorphicExtensions = {
                            RocksDbDataRegionConfigurationSchema.class, PageMemoryDataRegionConfigurationSchema.class,
                            UnsafeMemoryAllocatorConfigurationSchema.class
                    })
                    DataStorageConfiguration dbCfg
    ) throws Exception {
        NamedListView<TableView> oldValue = tablesCfg.tables().value();

        tablesCfg.tables().get("table").dataRegion().update("r0").get(1, TimeUnit.SECONDS);

        ValidationContext<NamedListView<TableView>> ctx = mockContext(oldValue, dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = validate(ctx);

        assertEquals(1, issuesCaptor.getAllValues().size());

        assertEquals(
                "Unable to move table 'schema.table' from region 'default' to region 'r0' because it has"
                        + " different type (old=rocksdb, new=pagemem)",
                issuesCaptor.getValue().message()
        );
    }

    /**
     * Tests that column names and column keys inside a Named List must be equal.
     */
    @Test
    void testMisalignedColumnNamedListKeys(
            @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class) DataStorageConfiguration dbCfg
    ) {
        NamedListView<TableView> oldValue = tablesCfg.tables().value();

        TableConfiguration tableCfg = tablesCfg.tables().get("table");

        CompletableFuture<Void> tableChangeFuture = tableCfg.columns()
                .change(columnsChange -> columnsChange
                        .create("ololo", columnChange -> columnChange
                                .changeName("not ololo")
                                .changeType(columnTypeChange -> columnTypeChange.changeType("STRING"))
                                .changeNullable(true)));

        assertThat(tableChangeFuture, willBe(nullValue(Void.class)));

        ValidationContext<NamedListView<TableView>> ctx = mockContext(oldValue, dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = validate(ctx);

        assertThat(issuesCaptor.getAllValues(), hasSize(1));

        assertThat(
                issuesCaptor.getValue().message(),
                is(equalTo("Column name \"not ololo\" does not match its Named List key: \"ololo\""))
        );
    }

    /**
     * Tests that index names and index keys inside a Named List must be equal.
     */
    @Test
    void testMisalignedIndexNamedListKeys(
            @InjectConfiguration(polymorphicExtensions = RocksDbDataRegionConfigurationSchema.class) DataStorageConfiguration dbCfg
    ) {
        NamedListView<TableView> oldValue = tablesCfg.tables().value();

        TableConfiguration tableCfg = tablesCfg.tables().get("table");

        CompletableFuture<Void> tableChangeFuture = tableCfg.indices()
                .change(indicesChange -> indicesChange
                        .create("ololo", indexChange -> indexChange
                                .changeName("not ololo")
                                .convert(HashIndexChange.class)
                                .changeColNames("id")));

        assertThat(tableChangeFuture, willBe(nullValue(Void.class)));

        ValidationContext<NamedListView<TableView>> ctx = mockContext(oldValue, dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = validate(ctx);

        assertThat(issuesCaptor.getAllValues(), hasSize(1));

        assertThat(
                issuesCaptor.getValue().message(),
                is(equalTo("Index name \"not ololo\" does not match its Named List key: \"ololo\""))
        );
    }

    /**
     * Mocks validation context.
     *
     * @param oldValue  Old value of configuration.
     * @param dbCfgView Data storage configuration to register it by {@link DataStorageConfiguration#KEY}.
     * @return Mocked validation context.
     */
    private ValidationContext<NamedListView<TableView>> mockContext(
            @Nullable NamedListView<TableView> oldValue,
            DataStorageView dbCfgView
    ) {
        ValidationContext<NamedListView<TableView>> ctx = mock(ValidationContext.class);

        NamedListView<TableView> newValue = tablesCfg.tables().value();

        when(ctx.getOldValue()).thenReturn(oldValue);
        when(ctx.getNewValue()).thenReturn(newValue);

        when(ctx.getOldRoot(DataStorageConfiguration.KEY)).thenReturn(dbCfgView);
        when(ctx.getNewRoot(DataStorageConfiguration.KEY)).thenReturn(dbCfgView);

        return ctx;
    }

    private static ArgumentCaptor<ValidationIssue> validate(ValidationContext<NamedListView<TableView>> ctx) {
        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        return issuesCaptor;
    }
}
