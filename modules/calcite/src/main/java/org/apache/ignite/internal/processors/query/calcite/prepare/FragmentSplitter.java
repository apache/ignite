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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
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

/**
 *
 */
public class FragmentSplitter implements IgniteRelVisitor<IgniteRel> {
    /** */
    private RelMetadataQueryEx mq;

    /** */
    private List<Fragment> fragments;

    /** */
    private RelNode cutPoint;

    /** */
    public List<Fragment> go(Fragment fragment, RelNode cutPoint, RelMetadataQueryEx mq) {
        this.cutPoint = cutPoint;
        this.mq = mq;

        fragments = new ArrayList<>();

        try {
            fragments.add(new Fragment(visit(fragment.root())));

            Collections.reverse(fragments);

            return fragments;
        }
        finally {
            this.cutPoint = null;
            this.mq = null;

            fragments = null;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSender rel) {
        // a split may happen on BiRel inputs merge. A sender node cannot be a BiRel input.
        assert rel != cutPoint;

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input == newInput)
            return rel;

        return (IgniteRel) rel.copy(rel.getTraitSet(), ImmutableList.of(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        boolean split = rel == cutPoint;

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input != newInput) {
            RelTraitSet traits = rel.getTraitSet()
                .replace(IgniteMdDistribution.filter(mq, newInput, rel.getCondition()));

            rel = (IgniteFilter) rel.copy(traits, ImmutableList.of(newInput));
        }

        return split ? split(rel, rel.getTraitSet()) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        boolean split = rel == cutPoint;

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input != newInput) {
            RelTraitSet traits = rel.getTraitSet()
                .replace(IgniteMdDistribution.project(mq, newInput, rel.getProjects()));

            rel = (IgniteProject) rel.copy(traits, ImmutableList.of(newInput));
        }

        return split ? split(rel, rel.getTraitSet()) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteJoin rel) {
        boolean split = rel == cutPoint;

        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        RelNode newLeft = visit((IgniteRel) left);
        RelNode newRight = visit((IgniteRel) right);

        // Join requires input distribution and produces its own one.
        // It cannot change the distribution on a child node change.
        if (left != newLeft || right != newRight)
            rel = (IgniteJoin) rel.copy(rel.getTraitSet(), ImmutableList.of(newLeft, newRight));

        return split ? split(rel, rel.getTraitSet()) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        boolean split = rel == cutPoint;

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input != newInput)
            rel = (IgniteTableModify) rel.copy(rel.getTraitSet(), ImmutableList.of(newInput));

        return split ? split(rel, rel.getTraitSet()) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return rel == cutPoint ? split(rel, rel.getTraitSet()) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        // a split may happen on BiRel inputs merge. A receiver doesn't have a
        // physical mapping, so, its merge with any input cannot cause the split.
        assert rel != cutPoint;

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
        // a split may happen on BiRel inputs merge. A values node doesn't have a
        // physical mapping, so, its merge with any input cannot cause the split.
        assert rel != cutPoint;

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    /** */
    private IgniteRel split(IgniteRel input, RelTraitSet traits) {
        RelOptCluster cluster = input.getCluster();

        Fragment fragment = new Fragment(new IgniteSender(cluster, traits, input));

        fragments.add(fragment);

        return new IgniteReceiver(cluster, traits, input.getRowType(), fragment);
    }
}
