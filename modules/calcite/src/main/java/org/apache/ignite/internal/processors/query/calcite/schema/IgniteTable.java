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

package org.apache.ignite.internal.processors.query.calcite.schema;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.splitter.PartitionsDistributionRegistry;
import org.apache.ignite.internal.processors.query.calcite.splitter.SourceDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.CU;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable {
    private final String tableName;
    private final String cacheName;
    private final RowType rowType;

    public IgniteTable(String tableName, String cacheName, RowType rowType) {
        this.tableName = tableName;
        this.cacheName = cacheName;
        this.rowType = rowType;
    }

    /**
     * @return Table name;
     */
    public String tableName() {
        return tableName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType.asRelDataType(typeFactory);
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        RelTraitSet traitSet = cluster.traitSet().replace(IgniteRel.LOGICAL_CONVENTION)
                .replaceIf(DistributionTraitDef.INSTANCE, () -> distributionTrait(cluster.getPlanner().getContext()));
        return new IgniteLogicalTableScan(cluster, traitSet, relOptTable);
    }

    public DistributionTrait distributionTrait(Context context) {
        return IgniteDistributions.hash(rowType.distributionKeys(), IgniteDistributions.noOpFunction()); // TODO
    }

    public SourceDistribution sourceDistribution(Context context, boolean local) {
        int cacheId = CU.cacheId(cacheName);

        SourceDistribution res = new SourceDistribution();

        GridIntList localInputs = new GridIntList();
        localInputs.add(cacheId);
        res.localInputs = localInputs;

        if (!local) {
            PartitionsDistributionRegistry registry = context.unwrap(PartitionsDistributionRegistry.class);
            AffinityTopologyVersion topVer = context.unwrap(AffinityTopologyVersion.class);
            res.partitionMapping = registry.distributed(cacheId, topVer);
        }

        return res;
    }
}
