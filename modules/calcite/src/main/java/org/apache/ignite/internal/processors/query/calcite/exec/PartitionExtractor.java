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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.primitives.Primitives;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionAllNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionLiteralNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionNoneNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionOperandNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionParameterNode;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
public class PartitionExtractor extends IgniteRelShuttle {
    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final Deque<PartitionNode> stack = new ArrayDeque<>();

    /** */
    public PartitionExtractor(IgniteTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
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

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTrimExchange rel) {
        stack.push(PartitionAllNode.IGNORE);

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        stack.push(PartitionAllNode.IGNORE);

        return rel;
    }

    /** {@inheritDoc} */
    @Override protected IgniteRel processNode(IgniteRel rel) {
        IgniteRel res = super.processNode(rel);

        if (rel.getInputs().size() > 1) {
            List<PartitionNode> operands = new ArrayList<>();
            for (int i = 0; i < rel.getInputs().size(); ++i)
                operands.add(stack.pop());

            stack.push(PartitionOperandNode.createOrOperandNode(operands));
        }
        else if (rel.getInputs().isEmpty())
            stack.push(PartitionAllNode.INSTANCE);

        return res;
    }

    /** */
    public PartitionNode go(Fragment fragment) {
        if (!(fragment.root() instanceof IgniteSender))
            return PartitionAllNode.INSTANCE;

        if (fragment.mapping() == null || !fragment.mapping().colocated())
            return PartitionAllNode.INSTANCE;

        processNode(fragment.root());

        if (stack.isEmpty())
            return PartitionAllNode.INSTANCE;

        return stack.pop();
    }

    /** */
    private void processScan(IgniteRel rel) {
        assert rel instanceof ProjectableFilterableTableScan;

        RexNode condition = ((ProjectableFilterableTableScan)rel).condition();

        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);

        if (!tbl.distribution().function().affinity() || tbl.distribution().getKeys().size() != 1) {
            stack.push(PartitionAllNode.INSTANCE);

            return;
        }

        ImmutableIntList keys = tbl.distribution().getKeys();
        RelDataType rowType = tbl.getRowType(typeFactory);
        int cacheId = ((CacheTableDescriptor)tbl.descriptor()).cacheInfo().cacheId();

        List<Class<?>> types = new ArrayList<>(rowType.getFieldCount());
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (QueryUtils.KEY_FIELD_NAME.equals(field.getName()))
                keys = keys.append(field.getIndex());

            types.add(Primitives.wrap((Class<?>)typeFactory.getJavaClass(field.getType())));
        }

        List<Integer> requiredCols;
        if (((ProjectableFilterableTableScan)rel).requiredColumns() != null)
            requiredCols = ((ProjectableFilterableTableScan)rel).requiredColumns().asList();
        else
            requiredCols = Collections.emptyList();

        PartitionNode partNode = processCondition(condition, types, keys, requiredCols, cacheId);

        stack.push(partNode);
    }

    /** */
    private PartitionNode processCondition(
        RexNode condition,
        List<Class<?>> types,
        ImmutableIntList keys,
        List<Integer> requiredCols,
        int cacheId
    ) {
        if (!(condition instanceof RexCall))
            return PartitionAllNode.INSTANCE;

        SqlKind opKind = condition.getKind();
        List<RexNode> operands = ((RexCall)condition).getOperands();

        switch (opKind) {
            case IS_NULL: {
                RexNode left = removeCast(operands.get(0));

                if (!left.isA(SqlKind.LOCAL_REF))
                    return PartitionAllNode.INSTANCE;

                int idx = ((RexLocalRef)left).getIndex();
                if (!requiredCols.isEmpty())
                    idx = requiredCols.get(idx);

                if (!keys.contains(idx))
                    return PartitionAllNode.INSTANCE;
                else
                    return PartitionNoneNode.INSTANCE;
            }
            case EQUALS: {
                if (operands.size() != 2)
                    return PartitionAllNode.INSTANCE;

                RexNode left, right;
                if (removeCast(operands.get(0)).isA(SqlKind.LOCAL_REF)) {
                    left = operands.get(0);
                    right = removeCast(operands.get(1));
                }
                else {
                    left = removeCast(operands.get(1));
                    right = operands.get(0);
                }

                if (!left.isA(SqlKind.LOCAL_REF))
                    return PartitionAllNode.INSTANCE;

                if (!right.isA(SqlKind.LITERAL) && !right.isA(SqlKind.DYNAMIC_PARAM))
                    return PartitionAllNode.INSTANCE;

                int idx = ((RexLocalRef)left).getIndex();
                if (!requiredCols.isEmpty())
                    idx = requiredCols.get(idx);

                if (!keys.contains(idx))
                    return PartitionAllNode.INSTANCE;

                Class<?> fldType = types.get(idx);

                if (right.isA(SqlKind.LITERAL))
                    return new PartitionLiteralNode(cacheId, (RexLiteral)right, fldType);
                else
                    return new PartitionParameterNode(cacheId, (RexDynamicParam)right, fldType);
            }
            case SEARCH:
                RexNode condition0 = RexUtil.expandSearch(Commons.emptyCluster().getRexBuilder(), null, condition);

                return processCondition(condition0, types, keys, requiredCols, cacheId);
            case OR:
            case AND:
                List<PartitionNode> operands0 = ((RexCall)condition).getOperands().stream()
                    .map(node -> processCondition(node, types, keys, requiredCols, cacheId))
                    .collect(Collectors.toList());

                return opKind == SqlKind.OR ? PartitionOperandNode.createOrOperandNode(operands0)
                    : PartitionOperandNode.createAndOperandNode(operands0);
            default:
                return PartitionAllNode.INSTANCE;
        }
    }

    /** */
    private static RexNode removeCast(RexNode node) {
        if (node.isA(SqlKind.CAST)) {
            assert node instanceof RexCall;
            assert ((RexCall)node).operandCount() == 1;

            RexNode op0 = ((RexCall)node).getOperands().get(0);

            return op0.getType().getFamily() != SqlTypeFamily.NULL ? node : op0;
        }

        return node;
    }
}
