/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;

/**
 *
 */
public class TableScanNode extends RelGraphNode {
    private final List<String> tableName;

    private TableScanNode(List<String> tableName) {
        this.tableName = tableName;
    }

    public static TableScanNode create(IgniteTableScan rel) {
        return new TableScanNode(rel.getTable().getQualifiedName());
    }

    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        return ctx.getSchema().getTableForMember(tableName).toRel(ctx);
    }
}
