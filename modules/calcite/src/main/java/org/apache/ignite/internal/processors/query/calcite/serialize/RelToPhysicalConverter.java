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
import java.util.List;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.Expression;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;
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
        long fragmentId = rel.target().fragmentId();
        NodesMapping mapping = rel.target().mapping();
        DistributionFunction fun = rel.targetDistribution().function();
        ImmutableIntList keys = rel.targetDistribution().getKeys();

        return new SenderPhysicalRel(fragmentId, mapping, fun, keys, visit((IgniteRel) rel.getInput()));
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteFilter rel) {
        return new FilterPhysicalRel(DataType.fromType(rel.getRowType()),
            rexTranslator.translate(rel.getCondition()), visit((IgniteRel) rel.getInput()));
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
    @Override public PhysicalRel visit(IgniteTableScan rel) {
        return new TableScanPhysicalRel(rel.getTable().getQualifiedName());
    }

    /** {@inheritDoc} */
    @Override public PhysicalRel visit(IgniteReceiver rel) {
        return new ReceiverPhysicalRel(DataType.fromType(rel.getRowType()),
            rel.source().fragmentId(), rel.source().mapping().nodes(), rel.collations());
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
