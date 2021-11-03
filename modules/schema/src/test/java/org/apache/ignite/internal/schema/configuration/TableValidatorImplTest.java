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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageView;
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
 *
 */
@ExtendWith(ConfigurationExtension.class)
public class TableValidatorImplTest {
    /** Basic table configuration to mutate and then validate. */
    @InjectConfiguration("mock.tables.table {\n"
            + "    name = schema.table,\n"
            + "    columns.0 {name = id, type.type = STRING, nullable = true},\n"
            + "    primaryKey {columns = [id], affinityColumns = [id]}\n"
            + "}")
    private TablesConfiguration tableCfg;

    /** Tests that validator finds no issues in a simple valid configuration. */
    @Test
    public void testNoIssues(@InjectConfiguration DataStorageConfiguration dbCfg) {
        ValidationContext<NamedListView<TableView>> ctx = mockContext(null, tableCfg.tables().value(), dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        assertThat(issuesCaptor.getAllValues(), is(empty()));
    }

    /** Tests that the validator catches nonexistent data regions. */
    @Test
    public void testMissingDataRegion(@InjectConfiguration DataStorageConfiguration dbCfg) throws Exception {
        tableCfg.tables().get("table").dataRegion().update("r0").get(1, TimeUnit.SECONDS);

        ValidationContext<NamedListView<TableView>> ctx = mockContext(null, tableCfg.tables().value(), dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        assertEquals(1, issuesCaptor.getAllValues().size());

        assertEquals(
                "Data region 'r0' configured for table 'schema.table' isn't found",
                issuesCaptor.getValue().message()
        );
    }

    /** Tests that new data region must have the same type. */
    @Test
    public void testChangeDataRegionType(
            @InjectConfiguration("mock.regions.r0.type = foo") DataStorageConfiguration dbCfg
    ) throws Exception {
        NamedListView<TableView> oldValue = tableCfg.tables().value();

        tableCfg.tables().get("table").dataRegion().update("r0").get(1, TimeUnit.SECONDS);

        NamedListView<TableView> newValue = tableCfg.tables().value();

        ValidationContext<NamedListView<TableView>> ctx = mockContext(oldValue, newValue, dbCfg.value());

        ArgumentCaptor<ValidationIssue> issuesCaptor = ArgumentCaptor.forClass(ValidationIssue.class);

        doNothing().when(ctx).addIssue(issuesCaptor.capture());

        TableValidatorImpl.INSTANCE.validate(null, ctx);

        assertEquals(1, issuesCaptor.getAllValues().size());

        assertEquals(
                "Unable to move table 'schema.table' from region 'default' to region 'r0' because it has"
                        + " different type (old=rocksdb, new=foo)",
                issuesCaptor.getValue().message()
        );
    }

    /**
     * Mocks validation context.
     *
     * @param oldValue  Old value of configuration.
     * @param newValue  New value of configuration.
     * @param dbCfgView Data storage configuration to register it by {@link DataStorageConfiguration#KEY}.
     * @return Mocked validation context.
     */
    private static ValidationContext<NamedListView<TableView>> mockContext(
            @Nullable NamedListView<TableView> oldValue,
            NamedListView<TableView> newValue,
            DataStorageView dbCfgView
    ) {
        ValidationContext<NamedListView<TableView>> ctx = mock(ValidationContext.class);

        when(ctx.getOldValue()).thenReturn(oldValue);
        when(ctx.getNewValue()).thenReturn(newValue);

        when(ctx.getOldRoot(DataStorageConfiguration.KEY)).thenReturn(dbCfgView);
        when(ctx.getNewRoot(DataStorageConfiguration.KEY)).thenReturn(dbCfgView);

        return ctx;
    }
}
