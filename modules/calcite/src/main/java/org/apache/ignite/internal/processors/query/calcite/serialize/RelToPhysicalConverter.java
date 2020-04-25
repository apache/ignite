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

package org.apache.ignite.internal.processors.query.calcite.serialize;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.Expression;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggCallExp;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Converts RelNode tree to physical rel tree.
 */
public class RelToPhysicalConverter implements IgniteRelVisitor<PhysicalRel> {
    /** */
    private final RexToExpTranslator rexTranslator;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    public RelToPhysicalConverter(IgniteTypeFactory typeFactory) {
        this.typeFactory = typeFactory;

        rexTranslator = new RexToExpTranslator(typeFactory);
    }

    /** */
    public PhysicalRel go(IgniteRel root) {
        return visit(root);
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteSender rel) {
        DistributionFunction fun = rel.distribution().function();
        ImmutableIntList keys = rel.distribution().getKeys();

        return new SenderPhysicalRel(rel.targetFragmentId(), fun, keys, visit((IgniteRel) rel.getInput()));
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteFilter rel) {
        return new FilterPhysicalRel(DataType.fromType(rel.getRowType()),
            rexTranslator.translate(rel.getCondition()), visit((IgniteRel) rel.getInput()));
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteTrimExchange rel) {
        IgniteDistribution distr = rel.distribution();

        assert distr.getType() == RelDistribution.Type.HASH_DISTRIBUTED;

        return new TrimExchangePhysicalRel(distr.function(), distr.getKeys(), visit((IgniteRel) rel.getInput()));
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteProject rel) {
        return new ProjectPhysicalRel(DataType.fromType(rel.getInput().getRowType()),
            rexTranslator.translate(rel.getProjects()), visit((IgniteRel) rel.getInput()));
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteJoin rel) {
        DataType dataType = DataType.fromType(
            Commons.combinedRowType(
                typeFactory, rel.getLeft().getRowType(), rel.getRight().getRowType()));

        return new JoinPhysicalRel(dataType, visit((IgniteRel) rel.getLeft()), visit((IgniteRel) rel.getRight()), rexTranslator.translate(rel.getCondition()));
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteAggregate rel) {
        byte type = AggregatePhysicalRel.SINGLE;
        List<ImmutableBitSet> groupSets = rel.getGroupSets();
        List<AggCallExp> calls = Commons.transform(rel.getAggCallList(), rexTranslator::translate);
        PhysicalRel input = visit((IgniteRel) rel.getInput());
        DataType inputRowType = DataType.fromType(rel.getInput().getRowType());

        return new AggregatePhysicalRel(type, groupSets, calls, input, inputRowType);
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteMapAggregate rel) {
        byte type = AggregatePhysicalRel.MAP;
        List<ImmutableBitSet> groupSets = rel.getGroupSets();
        List<AggCallExp> calls = Commons.transform(
            rel.getAggCallList(), rexTranslator::translate);
        PhysicalRel input = visit((IgniteRel) rel.getInput());
        DataType inputRowType = DataType.fromType(rel.getInput().getRowType());

        return new AggregatePhysicalRel(type, groupSets, calls, input, inputRowType);
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteReduceAggregate rel) {
        byte type = AggregatePhysicalRel.REDUCE;
        List<ImmutableBitSet> groupSets = rel.groupSets();
        List<AggCallExp> calls = Commons.transform(
            rel.aggregateCalls(), rexTranslator::translate);
        PhysicalRel input = visit((IgniteRel) rel.getInput());
        DataType inputRowType = null;

        return new AggregatePhysicalRel(type, groupSets, calls, input, inputRowType);
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteTableScan rel) {
        return new TableScanPhysicalRel(rel.getTable().getQualifiedName());
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteReceiver rel) {
        return new ReceiverPhysicalRel(DataType.fromType(rel.getRowType()),
            rel.exchangeId(), rel.sourceFragmentId(), rel.collations());
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteTableModify rel) {
        return new TableModifyPhysicalRel(rel.getTable().getQualifiedName(),
            rel.getOperation(), rel.getUpdateColumnList(), visit((IgniteRel) rel.getInput()));
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteValues rel) {
        return new ValuesPhysicalRel(toValues(rel.getTuples()), rel.getRowType().getFieldCount());
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteUnionAll rel) {
        List<PhysicalRel> inputs = new ArrayList<>(rel.getInputs().size());

        for (RelNode input : rel.getInputs())
            inputs.add(visit((IgniteRel) input));

        return new UnionAllPhysicalRel(inputs);
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** */
    private List<Expression> toValues(ImmutableList<ImmutableList<RexLiteral>> tuples) {
        return rexTranslator.translate(Commons.flat(Commons.cast(tuples)));
    }
}
