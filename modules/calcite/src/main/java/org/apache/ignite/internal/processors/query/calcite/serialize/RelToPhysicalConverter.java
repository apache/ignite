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

/**
 * Converts RelNode tree to physical rel tree.
 */
public class RelToPhysicalConverter /*implements IgniteRelVisitor<PhysicalRel>*/ {
//    /** */
//    private final RexToExpTranslator rexTranslator;
//
//    /** */
//    private final IgniteTypeFactory typeFactory;
//
//    /** */
//    public RelToPhysicalConverter(IgniteTypeFactory typeFactory) {
//        this.typeFactory = typeFactory;
//
//        rexTranslator = new RexToExpTranslator(typeFactory);
//    }
//
//    /** */
//    public PhysicalRel go(IgniteRel root) {
//        return visit(root);
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteSender rel) {
//        long fragmentId = rel.target().fragmentId();
//        NodesMapping mapping = rel.target().mapping();
//        DistributionFunction fun = rel.distribution().function();
//        ImmutableIntList keys = rel.distribution().getKeys();
//
//        return new SenderPhysicalRel(fragmentId, mapping, fun, keys, visit((IgniteRel) rel.getInput()));
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteFilter rel) {
//        return new FilterPhysicalRel(fromType(rel.getRowType()),
//            rexTranslator.translate(rel.getCondition()), visit((IgniteRel) rel.getInput()));
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteProject rel) {
//        return new ProjectPhysicalRel(fromType(rel.getInput().getRowType()),
//            rexTranslator.translate(rel.getProjects()), visit((IgniteRel) rel.getInput()));
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteJoin rel) {
//        DataType dataType = fromType(
//            Commons.combinedRowType(
//                typeFactory, rel.getLeft().getRowType(), rel.getRight().getRowType()));
//
//        return new JoinPhysicalRel(dataType, visit((IgniteRel) rel.getLeft()), visit((IgniteRel) rel.getRight()), rexTranslator.translate(rel.getCondition()));
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteTableScan scan) {
//        List<Expression> filters = scan.filters() == null ? null : rexTranslator.translate(scan.filters());
//
//        List<Expression> lowerBound = scan.lowerIndexCondition() == null ? null :
//            rexTranslator.translate(scan.lowerIndexCondition());
//        List<Expression> upperBound = scan.upperIndexCondition() == null ? null :
//            rexTranslator.translate(scan.upperIndexCondition());
//
//        return new TableScanPhysicalRel(scan.getTable().getQualifiedName(),
//            scan.indexName(),
//            fromType(scan.getRowType()),
//            filters,
//            lowerBound,
//            upperBound);
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteReceiver rel) {
//        return new ReceiverPhysicalRel(fromType(rel.getRowType()),
//            rel.source().fragmentId(), rel.source().mapping().nodes(), rel.collations());
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteTableModify rel) {
//        return new TableModifyPhysicalRel(rel.getTable().getQualifiedName(),
//            rel.getOperation(), rel.getUpdateColumnList(), visit((IgniteRel) rel.getInput()));
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteValues rel) {
//        return new ValuesPhysicalRel(toValues(rel.getTuples()), rel.getRowType().getFieldCount());
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteExchange rel) {
//        throw new AssertionError();
//    }
//
//    /** {@inheritDoc} */
//    @Override public PhysicalRel visit(IgniteRel rel) {
//        return rel.accept(this);
//    }
//
//    /** */
//    private List<Expression> toValues(ImmutableList<ImmutableList<RexLiteral>> tuples) {
//        return rexTranslator.translate(Commons.flat(Commons.cast(tuples)));
//    }
}
