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

import org.apache.ignite.internal.processors.metric.list.walker.Order;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2IndexType;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.index.Index;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Sql index representation for a {@link MonitoringList}.
 */
public class SqlIndexView implements MonitoringRow<String> {
    /** */
    private final Index idx;

    /** */
    private final GridH2Table tbl;

    /** */
    private final H2IndexType type;

    /** */
    private final Integer inlineSz;

    /** */
    private String cacheGrpName;

    /** */
    public SqlIndexView(GridH2Table tbl, String cacheGrpName, Index idx, H2IndexType type, Integer inlineSz) {
        this.tbl = tbl;
        this.cacheGrpName = cacheGrpName;
        this.idx = idx;
        this.type = type;
        this.inlineSz = inlineSz;
    }

    /** {@inheritDoc} */
    @Override public String monitoringRowId() {
        return metricName(tbl.identifierString(), indexName());
    }

    /** */
    public int cacheGroupId() {
        return tbl.cacheInfo().groupId();
    }

    /** */
    public String cacheGroupName() {
        return cacheGrpName;
    }

    /** */
    public int cacheId() {
        return tbl.cacheId();
    }

    /** */
    @Order(5)
    public String cacheName() {
        return tbl.cacheName();
    }

    /** */
    @Order(3)
    public String schemaName() {
        return tbl.schemaName();
    }

    /** */
    @Order(4)
    public String tableName() {
        return tbl.identifier().table();
    }

    /** */
    @Order()
    public String indexName() {
        return idx.getName();
    }

    /** */
    @Order(1)
    public H2IndexType indexType() {
        return type;
    }

    /** */
    @Order(2)
    public String columns() {
        switch (type) {
            case HASH:
            case BTREE:
                return H2Utils.indexColumnsSql(H2Utils.unwrapKeyColumns(tbl, idx.getIndexColumns()));

            case SPATIAL:
                return H2Utils.indexColumnsSql(idx.getIndexColumns());

            case SCAN:
                return null;

            default:
                return "???";
        }
    }

    /** */
    public boolean isPk() {
        return idx.getIndexType().isPrimaryKey();
    }

    /** */
    public boolean isUnique() {
        return idx.getIndexType().isUnique();
    }

    /** */
    public Integer inlineSize() {
        return inlineSz;
    }
}
