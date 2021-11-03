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

package org.apache.ignite.client.fakes;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.NotNull;

/**
 * Fake tables.
 */
public class FakeIgniteTables implements IgniteTables, IgniteTablesInternal {
    /**
     *
     */
    public static final String TABLE_EXISTS = "Table exists";
    
    /**
     *
     */
    private final ConcurrentHashMap<String, TableImpl> tables = new ConcurrentHashMap<>();
    
    /**
     *
     */
    private final ConcurrentHashMap<IgniteUuid, TableImpl> tablesById = new ConcurrentHashMap<>();
    
    /** {@inheritDoc} */
    @Override
    public Table createTable(String name, Consumer<TableChange> tableInitChange) {
        var newTable = getNewTable(name);
        
        var oldTable = tables.putIfAbsent(name, newTable);
    
        if (oldTable != null) {
            throw new IgniteException(TABLE_EXISTS);
        }
        
        tablesById.put(newTable.tableId(), newTable);
        
        return newTable;
    }
    
    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> createTableAsync(String name, Consumer<TableChange> tableInitChange) {
        return CompletableFuture.completedFuture(createTable(name, tableInitChange));
    }
    
    /** {@inheritDoc} */
    @Override
    public void alterTable(String name, Consumer<TableChange> tableChange) {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public CompletableFuture<Void> alterTableAsync(String name, Consumer<TableChange> tableChange) {
        throw new UnsupportedOperationException();
    }
    
    /** {@inheritDoc} */
    @Override
    public Table createTableIfNotExists(String name, Consumer<TableChange> tableInitChange) {
        var newTable = getNewTable(name);
        
        var oldTable = tables.putIfAbsent(name, newTable);
    
        if (oldTable != null) {
            return oldTable;
        }
        
        tablesById.put(newTable.tableId(), newTable);
        
        return newTable;
    }
    
    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> createTableIfNotExistsAsync(String name, Consumer<TableChange> tableInitChange) {
        return CompletableFuture.completedFuture(createTableIfNotExists(name, tableInitChange));
    }
    
    /** {@inheritDoc} */
    @Override
    public void dropTable(String name) {
        var table = tables.remove(name);
    
        if (table != null) {
            tablesById.remove(table.tableId());
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropTableAsync(String name) {
        dropTable(name);
        
        return CompletableFuture.completedFuture(null);
    }
    
    /** {@inheritDoc} */
    @Override
    public List<Table> tables() {
        return new ArrayList<>(tables.values());
    }
    
    /** {@inheritDoc} */
    @Override
    public CompletableFuture<List<Table>> tablesAsync() {
        return CompletableFuture.completedFuture(tables());
    }
    
    /** {@inheritDoc} */
    @Override
    public Table table(String name) {
        return tables.get(name);
    }
    
    /** {@inheritDoc} */
    @Override
    public TableImpl table(IgniteUuid id) {
        return tablesById.get(id);
    }
    
    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Table> tableAsync(String name) {
        return CompletableFuture.completedFuture(table(name));
    }
    
    /** {@inheritDoc} */
    @Override
    public CompletableFuture<TableImpl> tableAsync(IgniteUuid id) {
        return CompletableFuture.completedFuture(tablesById.get(id));
    }
    
    @NotNull
    private TableImpl getNewTable(String name) {
        return new TableImpl(
                new FakeInternalTable(name, new IgniteUuid(UUID.randomUUID(), 0)),
                new FakeSchemaRegistry(this::getSchema),
                null
        );
    }
    
    /**
     * Gets the schema.
     *
     * @param v Version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor getSchema(Integer v) {
        switch (v) {
            case 1:
                return new SchemaDescriptor(
                        1,
                        new Column[]{new Column("id", NativeTypes.INT64, false)},
                        new Column[]{new Column("name", NativeTypes.STRING, true)});
            
            case 2:
                return new SchemaDescriptor(
                        2,
                        new Column[]{new Column("id", NativeTypes.INT64, false)},
                        new Column[]{
                                new Column("name", NativeTypes.STRING, true),
                                new Column("xyz", NativeTypes.STRING, true)
                        });
            default:
                return null;
        }
    }
}
