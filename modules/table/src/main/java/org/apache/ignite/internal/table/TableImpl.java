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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.schema.definition.SchemaManagementMode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl implements Table {
    /** Table manager. */
    private final TableManager tblMgr;

    /** Internal table. */
    private final InternalTable tbl;

    /** Schema registry. */
    private final SchemaRegistry schemaReg;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param schemaReg Table schema registry.
     * @param tblMgr Table manager.
     */
    public TableImpl(InternalTable tbl, SchemaRegistry schemaReg, TableManager tblMgr) {
        this.tbl = tbl;
        this.schemaReg = schemaReg;
        this.tblMgr = tblMgr;
    }

    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    public @NotNull IgniteUuid tableId() {
        return tbl.tableId();
    }

    /** Returns an internal table instance this view represents. */
    public InternalTable internalTable() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return tbl.tableName();
    }

    /**
     * Gets a schema view for the table.
     *
     * @return Schema view.
     */
    public SchemaRegistry schemaView() {
        return schemaReg;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, schemaReg, recMapper, null);
    }

    /** {@inheritDoc} */
    @Override public RecordView<Tuple> recordView() {
        return new RecordBinaryViewImpl(tbl, schemaReg, tblMgr, null);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> keyValueView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KeyValueViewImpl<>(tbl, schemaReg, keyMapper, valMapper, null);
    }

    /** {@inheritDoc} */
    @Override public KeyValueView<Tuple, Tuple> keyValueView() {
        return new KeyValueBinaryViewImpl(tbl, schemaReg, tblMgr, null);
    }

    /**
     * @param schemaMode New schema management mode.
     */
    public void schemaMode(SchemaManagementMode schemaMode) {
        this.tbl.schema(schemaMode);
    }

    /**
     * Updates internal table raft group service for given partition.
     *
     * @param p Partition.
     * @param raftGrpSvc Raft group service.
     */
    public void updateInternalTableRaftGroupService(int p, RaftGroupService raftGrpSvc) {
        ((InternalTableImpl)tbl).updateInternalTableRaftGroupService(p, raftGrpSvc);
    }
}
