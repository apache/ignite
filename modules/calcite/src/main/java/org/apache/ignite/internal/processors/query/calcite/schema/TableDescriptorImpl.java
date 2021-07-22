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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;

/**
 *
 */
public class TableDescriptorImpl extends NullInitializerExpressionFactory implements TableDescriptor {
    /** */
    private static final ColumnDescriptor[] DUMMY = new ColumnDescriptor[0];

    private final TopologyService topSrvc;

    /** */
    private final ColumnDescriptor[] descriptors;

    /** */
    private final Map<String, ColumnDescriptor> descriptorsMap;

    /** */
    private final ImmutableBitSet insertFields;

    /** */
    public TableDescriptorImpl(
        TopologyService topSrvc,
        List<ColumnDescriptor> columnDescriptors
    ) {
        Map<String, ColumnDescriptor> descriptorsMap = new HashMap<>(columnDescriptors.size());
        for (ColumnDescriptor descriptor : columnDescriptors)
            descriptorsMap.put(descriptor.name(), descriptor);

        this.descriptors = columnDescriptors.toArray(DUMMY);
        this.descriptorsMap = descriptorsMap;
        this.topSrvc = topSrvc;

        insertFields = ImmutableBitSet.range(columnDescriptors.size());
    }

    /** {@inheritDoc} */
    @Override public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, insertFields);
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return IgniteDistributions.random();
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public ColumnStrategy generationStrategy(RelOptTable tbl, int colIdx) {
        if (descriptors[colIdx].hasDefaultValue())
            return ColumnStrategy.DEFAULT;

        return super.generationStrategy(tbl, colIdx);
    }

    /** {@inheritDoc} */
    @Override public RexNode newColumnDefaultValue(RelOptTable tbl, int colIdx, InitializerContext ctx) {
        final ColumnDescriptor desc = descriptors[colIdx];

        if (!desc.hasDefaultValue())
            return super.newColumnDefaultValue(tbl, colIdx, ctx);

        final RexBuilder rexBuilder = ctx.getRexBuilder();
        final IgniteTypeFactory typeFactory = (IgniteTypeFactory) rexBuilder.getTypeFactory();

        return rexBuilder.makeLiteral(desc.defaultValue(), desc.logicalType(typeFactory), false);
    }

    /** {@inheritDoc} */
    @Override public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        if (usedColumns == null) {
            for (int i = 0; i < descriptors.length; i++)
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }
        else {
            for (int i = usedColumns.nextSetBit(0); i != -1; i = usedColumns.nextSetBit(i + 1))
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }

        return TypeUtils.sqlType(factory, b.build());
    }

    /** {@inheritDoc} */
    @Override public ColumnDescriptor columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup colocationGroup(PlanningContext ctx) {
        return partitionedGroup();
    }

    /** */
    private ColocationGroup partitionedGroup() {
        List<List<String>> assignments = topSrvc.allMembers().stream()
            .map(ClusterNode::id)
            .map(Collections::singletonList)
            .collect(Collectors.toList());

        return ColocationGroup.forAssignments(assignments);
    }
}
