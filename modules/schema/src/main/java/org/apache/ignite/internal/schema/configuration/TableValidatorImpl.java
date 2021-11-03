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

import static org.apache.ignite.configuration.schemas.store.DataStorageConfigurationSchema.DEFAULT_DATA_REGION_NAME;

import java.util.Objects;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.store.DataRegionView;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageView;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.internal.schema.definition.builder.TableDefinitionBuilderImpl;
import org.jetbrains.annotations.Nullable;

/**
 * Table schema configuration validator implementation.
 */
public class TableValidatorImpl implements Validator<TableValidator, NamedListView<TableView>> {
    /** Static instance. */
    public static final TableValidatorImpl INSTANCE = new TableValidatorImpl();
    
    /** {@inheritDoc} */
    @Override
    public void validate(TableValidator annotation, ValidationContext<NamedListView<TableView>> ctx) {
        NamedListView<TableView> oldTables = ctx.getOldValue();
        NamedListView<TableView> newTables = ctx.getNewValue();
        
        for (String tableName : newTables.namedListKeys()) {
            TableView newTable = newTables.get(tableName);
            
            try {
                TableDefinitionImpl tbl = SchemaConfigurationConverter.convert(newTable);
                
                assert !tbl.keyColumns().isEmpty();
                assert !tbl.affinityColumns().isEmpty();
                
                TableDefinitionBuilderImpl.validateIndices(tbl.indices(), tbl.columns(), tbl.affinityColumns());
            } catch (IllegalArgumentException e) {
                ctx.addIssue(new ValidationIssue("Validator works success by key " + ctx.currentKey() + ". Found "
                        + newTable.columns().size() + " columns"));
            }
            
            validateDataRegion(oldTables == null ? null : oldTables.get(tableName), newTable, ctx);
        }
    }
    
    /**
     * Checks that data region configuration is valid. Check involves data region existence and region type preservation if it's updated.
     *
     * @param oldTable Previous configuration, maybe {@code null}.
     * @param newTable New configuration.
     * @param ctx      Validation context.
     */
    private void validateDataRegion(@Nullable TableView oldTable, TableView newTable, ValidationContext<?> ctx) {
        DataStorageView oldDbCfg = ctx.getOldRoot(DataStorageConfiguration.KEY);
        DataStorageView newDbCfg = ctx.getNewRoot(DataStorageConfiguration.KEY);
    
        if (oldTable != null && Objects.equals(oldTable.dataRegion(), newTable.dataRegion())) {
            return;
        }
        
        DataRegionView newRegion = dataRegion(newDbCfg, newTable.dataRegion());
        
        if (newRegion == null) {
            ctx.addIssue(new ValidationIssue(String.format(
                    "Data region '%s' configured for table '%s' isn't found",
                    newTable.dataRegion(),
                    newTable.name()
            )));
            
            return;
        }
    
        if (oldDbCfg == null || oldTable == null) {
            return;
        }
        
        DataRegionView oldRegion = dataRegion(oldDbCfg, oldTable.dataRegion());
        
        if (!oldRegion.type().equalsIgnoreCase(newRegion.type())) {
            ctx.addIssue(new ValidationIssue(String.format(
                    "Unable to move table '%s' from region '%s' to region '%s' because it has different type (old=%s, new=%s)",
                    newTable.name(),
                    oldTable.dataRegion(),
                    newTable.dataRegion(),
                    oldRegion.type(),
                    newRegion.type()
            )));
        }
    }
    
    /**
     * Retrieves data region configuration.
     *
     * @param dbCfg      Data storage configuration.
     * @param regionName Data region name.
     * @return Data region configuration.
     */
    private static DataRegionView dataRegion(DataStorageView dbCfg, String regionName) {
        if (regionName.equals(DEFAULT_DATA_REGION_NAME)) {
            return dbCfg.defaultRegion();
        }
        
        return dbCfg.regions().get(regionName);
    }
    
    /** Private constructor. */
    private TableValidatorImpl() {
    }
}
