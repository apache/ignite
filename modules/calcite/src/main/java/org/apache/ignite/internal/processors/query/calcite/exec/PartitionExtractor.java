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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Primitives;
import org.apache.calcite.DataContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionAllNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionLiteralNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionOperandNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionParameterNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionPruningContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseDataContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class PartitionExtractor extends IgniteRelShuttle {
    /** */
    private final AffinityService affSvc;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final DataContext dataContext;

    /** */
    private final Map<String, Object> params;

    /** */
    private PartitionNode partNode;

    /** */
    public PartitionExtractor(AffinityService affSvc, IgniteTypeFactory typeFactory, Map<String, Object> params) {
        this.affSvc = affSvc;
        this.typeFactory = typeFactory;
        dataContext = new BaseDataContext(typeFactory);
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        processScan(rel);

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        processScan(rel);

        return rel;
    }

    /** */
    public int[] go(Fragment fragment) {
        if (!(fragment.root() instanceof IgniteSender))
            return null;

        if (fragment.mapping() == null || !fragment.mapping().colocated())
            return null;

        visit(fragment.root());

        if (partNode != null) {
            partNode = partNode.optimize();

            Collection<Integer> parts = partNode.apply(new PartitionPruningContext(affSvc, dataContext, params));

            if (F.isEmpty(parts))
                return null;

            int[] parts0 = Ints.toArray(parts);

            Arrays.sort(parts0);

            return parts0;
        }

        return null;
    }

    /** */
    private void processScan(IgniteRel rel) {
        if (partNode == PartitionAllNode.INSTANCE)
            return;

        assert rel instanceof ProjectableFilterableTableScan;

        RexNode condition = ((ProjectableFilterableTableScan)rel).condition();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);

        if (!tbl.descriptor().distribution().function().affinity()) {
            partNode = PartitionAllNode.INSTANCE;

            return;
        }

        RelDataType rowType = tbl.getRowType(typeFactory);

        List<Class<?>> types = new ArrayList<>(rowType.getFieldCount());
        for (RelDataType type : RelOptUtil.getFieldTypeList(rowType))
            types.add(Primitives.wrap((Class<?>)typeFactory.getJavaClass(type)));

        int cacheId = ((CacheTableDescriptor)tbl.descriptor()).cacheInfo().cacheId();

        PartitionNode partNode0 = processCondition(condition, types, rel.distribution().getKeys(), cacheId);

        if (partNode == null) {
            partNode = partNode0;
        }
        else {
            partNode = PartitionOperandNode.createOrOperandNode(ImmutableList.of(partNode0, partNode));
        }
    }

    /** */
    private PartitionNode processCondition(RexNode condition, List<Class<?>> types, ImmutableIntList keys, int cacheId) {
        if (!(condition instanceof RexCall) || keys.size() != 1)
            return PartitionAllNode.INSTANCE;

        SqlKind opKind = condition.getKind();
        List<RexNode> operands = ((RexCall)condition).getOperands();

        switch (opKind) {
            case EQUALS:
                if (operands.size() != 2)
                    return PartitionAllNode.INSTANCE;

                RexNode left = operands.get(0);
                RexNode right = operands.get(1);

                if (!left.isA(SqlKind.LOCAL_REF))
                    return PartitionAllNode.INSTANCE;

                if (!right.isA(SqlKind.LITERAL) && !right.isA(SqlKind.DYNAMIC_PARAM))
                    return PartitionAllNode.INSTANCE;

                int idx = ((RexLocalRef)left).getIndex();

                if (!keys.contains(idx))
                    return PartitionAllNode.INSTANCE;

                Class<?> fldType = types.get(idx + 2);

                if (right.isA(SqlKind.LITERAL))
                    return new PartitionLiteralNode(cacheId, (RexLiteral)right, fldType);
                else
                    return new PartitionParameterNode(cacheId, (RexDynamicParam)right, fldType);
            case SEARCH:
                RexNode condition0 = RexUtil.expandSearch(Commons.emptyCluster().getRexBuilder(), null, condition);

                return processCondition(condition0, types, keys, cacheId);
            case OR:
            case AND:
                List<PartitionNode> operands0 = ((RexCall)condition).getOperands().stream()
                    .map(node -> processCondition(node, types, keys, cacheId))
                    .collect(Collectors.toList());

                return opKind == SqlKind.OR ? PartitionOperandNode.createOrOperandNode(operands0)
                    : PartitionOperandNode.createAndOperandNode(operands0);
            default:
                return PartitionAllNode.INSTANCE;
        }
    }

    /** */
    private Object createKey(IgniteTable tbl, ImmutableIntList keys, Object... args) {
        List<CacheColumnDescriptor> descriptors = tbl.descriptor().columnDescriptors()
                .stream().map(d -> (CacheColumnDescriptor)d).collect(Collectors.toList());

        List<CacheColumnDescriptor> keyDescriptors = keys.stream().map(idx -> descriptors.get(idx + 2))
                .collect(Collectors.toList());

        GridCacheContext<?, ?> cctx = ((CacheTableDescriptor)tbl.descriptor()).cacheContext();
        GridQueryTypeDescriptor typeDesc = ((CacheTableDescriptor)tbl.descriptor()).typeDescription();

        if (args == null)
            return null;

        Object key;
        if (args.length > 1) {
            key = TypeUtils.createObject(cctx, typeDesc.keyTypeName(), typeDesc.keyClass());

            for (int i = 0; i < args.length; ++i) {
                CacheColumnDescriptor desc = keyDescriptors.get(i);

                try {
                    desc.set(key, TypeUtils.fromInternal(dataContext, args[i], desc.storageType()));
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }
        else {
            CacheColumnDescriptor desc = keyDescriptors.get(0);

            key = TypeUtils.fromInternal(dataContext, args[0], desc.storageType());
        }

        if (cctx.binaryMarshaller() && key instanceof BinaryObjectBuilder)
            key = ((BinaryObjectBuilder)key).build();

        return key;
    }
}
