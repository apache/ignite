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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.List;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;

/**
 * Describes {@link IgniteTableScan}.
 */
public class TableScanNode extends RelGraphNode {
    /** */
    private final List<String> tableName;

    /**
     * @param traits   Traits of this relational expression
     * @param tableName Qualified table name
     */
    private TableScanNode(RelTraitSet traits, List<String> tableName) {
        super(traits);
        this.tableName = tableName;
    }

    /**
     * Factory method.
     *
     * @param rel TableScan rel.
     * @return TableScanNode.
     */
    public static TableScanNode create(IgniteTableScan rel) {
        return new TableScanNode(rel.getTraitSet(), rel.getTable().getQualifiedName());
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        return new IgniteTableScan(ctx.getCluster(),
            traits.toTraitSet(ctx.getCluster()),
            ctx.getSchema().getTableForMember(tableName));
    }
}
