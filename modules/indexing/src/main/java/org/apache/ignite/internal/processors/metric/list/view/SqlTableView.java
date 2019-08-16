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

package org.apache.ignite.internal.processors.metric.list.view;

import org.apache.ignite.internal.processors.metric.list.MonitoringRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/** */
public class SqlTableView implements MonitoringRow<String> {
    /** */
    private final String cacheGrpName;

    /** */
    private final GridH2Table tbl;

    /** */
    public SqlTableView(String cacheGrpName, GridH2Table tbl) {
        this.cacheGrpName = cacheGrpName;

        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public String sessionId() {
        return "unknown";
    }

    /** */
    public int cacheGrpId() {
        return tbl.cacheInfo().groupId();
    }

    /** */
    public String cacheGrpName() {
        return cacheGrpName;
    }

    /** */
    public int cacheId() {
        return tbl.cacheId();
    }

    /** */
    public String cacheName() {
        return tbl.cacheName();
    }

    /** */
    public String schemaName() {
        return tbl.getSchema().getName();
    }

    /** */
    public String tableName() {
        return tbl.getName();
    }

    /** */
    public String affKeyCol() {
        IndexColumn affCol = tbl.getAffinityKeyColumn();

        if (affCol == null)
            return null;

        // Only explicit affinity column should be shown. Do not do this for _KEY or it's alias.
        if (tbl.rowDescriptor().isKeyColumn(affCol.column.getColumnId()))
            return null;

        return affCol.columnName;
    }

    /** */
    public String keyAlias() {
        return tbl.rowDescriptor().type().keyFieldAlias();
    }

    /** */
    public String valAlias() {
        return tbl.rowDescriptor().type().valueFieldAlias();
    }

    /** */
    public String keyTypeName() {
        return tbl.rowDescriptor().type().keyTypeName();
    }

    /** */
    public String valTypeName() {
        return tbl.rowDescriptor().type().valueTypeName();
    }
}
