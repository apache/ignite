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

package org.apache.ignite.internal.processors.query.calcite.splitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.RelOp;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.igniteRel;

/**
 * Splits a query into a list of query fragments.
 */
public class Splitter implements IgniteRelVisitor<IgniteRel>, RelOp<IgniteRel, QueryPlan> {
    /** */
    private List<Fragment> fragments;

    /** {@inheritDoc} */
    @Override public QueryPlan go(IgniteRel root) {
        fragments = new ArrayList<>();

        fragments.add(new Fragment(visit(root)));

        Collections.reverse(fragments);

        return new QueryPlan(fragments);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        RelOptCluster cluster = rel.getCluster();
        IgniteRel input = visit(igniteRel(rel.getInput()));

        RelTraitSet inTraits = input.getTraitSet();
        RelTraitSet outTraits = rel.getTraitSet();

        Fragment fragment = new Fragment(new IgniteSender(cluster, inTraits, input));

        fragments.add(fragment);

        return new IgniteReceiver(cluster, outTraits, input.getRowType(), fragment);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        return visitChild(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        return visitChild(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteJoin rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        throw new AssertionError("An attempt to split an already split task.");
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSender rel) {
        throw new AssertionError("An attempt to split an already split task.");
    }

    /**
     * Visits all children of a parent.
     */
    private IgniteRel visitChildren(IgniteRel rel) {
        for (Ord<RelNode> input : Ord.zip(rel.getInputs()))
            visitChild(rel, input.i, igniteRel(input.e));

        return rel;
    }

    /**
     * Visits a single child of a parent.
     */
    private <T extends SingleRel & IgniteRel> IgniteRel visitChild(T rel) {
        visitChild(rel, 0, igniteRel(rel.getInput()));

        return rel;
    }

    /**
     * Visits a particular child of a parent and replaces the child if it was changed.
     */
    private void visitChild(IgniteRel parent, int i, IgniteRel child) {
        IgniteRel child2 = visit(child);
        if (child2 != child)
            parent.replaceInput(i, child2);
    }
}
