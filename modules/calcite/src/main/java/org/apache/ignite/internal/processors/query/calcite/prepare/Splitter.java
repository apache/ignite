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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
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
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;

/**
 * Splits a query into a list of query fragments.
 */
public class Splitter implements IgniteRelVisitor<IgniteRel> {
    /** */
    private List<Fragment> fragments;

    /** */
    public List<Fragment> go(IgniteRel root) {
        fragments = new ArrayList<>();

        try {
            fragments.add(new Fragment(visit(root)));

            Collections.reverse(fragments);

            return fragments;
        }
        finally {
            fragments = null;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteJoin rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteAggregate rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMapAggregate rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReduceAggregate rel) {
        return visitChildren(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
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

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        RelOptCluster cluster = rel.getCluster();
        IgniteRel input = visit((IgniteRel) rel.getInput());

        Fragment fragment = new Fragment(new IgniteSender(cluster, rel.getTraitSet(), input));

        fragments.add(fragment);

        return new IgniteReceiver(cluster, rel.getTraitSet(), input.getRowType(), fragment);
    }

    /**
     * Visits all children of a parent.
     */
    private <T extends RelNode> T visitChildren(RelNode rel) {
        List<RelNode> inputs = rel.getInputs();

        for (int i = 0; i < inputs.size(); i++)
            visitChild(rel, i, inputs.get(i));

        return (T)rel;
    }

    /**
     * Visits a particular child of a parent and replaces the child if it was changed.
     */
    private void visitChild(RelNode parent, int i, RelNode child) {
        IgniteRel newChild = visit((IgniteRel) child);

        if (newChild != child)
            parent.replaceInput(i, newChild);
    }
}
