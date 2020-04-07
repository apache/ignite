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
import java.util.Objects;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashFilter;
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
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class FragmentSplitter implements IgniteRelVisitor<IgniteRel> {
    /** */
    private RelMetadataQuery mq;

    /** */
    private List<Fragment> fragments;

    /** */
    private RelNode cutPoint;

    /** */
    public List<Fragment> go(Fragment fragment, RelNode cutPoint, RelMetadataQuery mq) {
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
        assert cutPoint != null;

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input == newInput)
            return rel;

        return (IgniteRel) rel.copy(rel.getTraitSet(), ImmutableList.of(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteAggregate rel) {
        boolean split = cutPoint(rel);

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input != newInput) {
            checkDistributionEqual(input, newInput);

            rel = (IgniteAggregate) rel.copy(rel.getTraitSet(), ImmutableList.of(newInput));
        }

        return split ? split(rel) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMapAggregate rel) {
        // a split may happen on BiRel inputs merge. An map aggregate node cannot be a BiRel input.
        assert rel != cutPoint;
        assert cutPoint != null;

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input == newInput)
            return rel;

        IgniteDistribution newDistr = IgniteDistributions.mapAggregate(mq,
            newInput, rel.getGroupSet(), rel.getGroupSets(), rel.getAggCallList());

        assert newDistr != null;

        RelTraitSet traits = rel.getTraitSet()
            .replace(newDistr);

        return (IgniteRel) rel.copy(traits, ImmutableList.of(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReduceAggregate rel) {
        // a split may happen on BiRel inputs merge. An reduce aggregate node doesn't have a
        // physical mapping (because it always goes after receiver node), so, its merge with
        // any input cannot cause the split.
        assert cutPoint == null;

        if (U.assertionsEnabled()) {
            RelNode input = rel.getInput();
            RelNode newInput = visit((IgniteRel) input);

            assert input == newInput;
        }

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        boolean split = cutPoint(rel);

        RelNode input = rel.getInput();
        IgniteRel newInput = visit((IgniteRel) input);

        if (input != newInput) {
            RelTraitSet traits = rel.getTraitSet()
                .replace(newInput.distribution());

            rel = (IgniteFilter) rel.copy(traits, ImmutableList.of(newInput));
        }

        return split ? split(rel) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteHashFilter rel) {
        boolean split = cutPoint(rel);

        // after split we need to remove the filter, return an original
        // distribution and propagate the distribution to a fragment root.
        if (split)
            return split((IgniteRel) rel.getInput());
        else if (cutPoint == null)
            return (IgniteRel) rel.getInput();

        RelNode input = rel.getInput();
        IgniteRel newInput = visit((IgniteRel) input);

        if (input == newInput)
            return rel;

        return (IgniteRel) rel.copy(rel.getTraitSet(), F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        boolean split = cutPoint(rel);

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input != newInput) {
            RelTraitSet traits = rel.getTraitSet()
                .replace(IgniteDistributions.project(mq, newInput, rel.getProjects()));

            rel = (IgniteProject) rel.copy(traits, ImmutableList.of(newInput));
        }

        return split ? split(rel) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteJoin rel) {
        if (cutPoint == null)
            return rel; // no need to check deeper

        boolean split = cutPoint(rel);

        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();

        RelNode newLeft = visit((IgniteRel) left);
        RelNode newRight = visit((IgniteRel) right);

        if (left != newLeft || right != newRight) {
            // Join requires input distribution and produces its own one.
            // It cannot change the distribution on a child node change.
            checkDistributionEqual(left, newLeft);
            checkDistributionEqual(right, newRight);

            rel = (IgniteJoin) rel.copy(rel.getTraitSet(), ImmutableList.of(newLeft, newRight));
        }

        return split ? split(rel) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        boolean split = cutPoint(rel);

        RelNode input = rel.getInput();
        RelNode newInput = visit((IgniteRel) input);

        if (input != newInput) {
            checkDistributionEqual(input, newInput);

            rel = (IgniteTableModify) rel.copy(rel.getTraitSet(), ImmutableList.of(newInput));
        }

        return split ? split(rel) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return cutPoint(rel) ? split(rel) : rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        // a split may happen on BiRel inputs merge. A receiver doesn't have a
        // physical mapping, so, its merge with any input cannot cause the split.
        assert cutPoint == null;

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
        // a split may happen on BiRel inputs merge. A values node doesn't have a
        // physical mapping, so, its merge with any input cannot cause the split.
        assert cutPoint == null;

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
    public boolean cutPoint(RelNode rel) {
        boolean res = rel == cutPoint;

        if (res)
            cutPoint = null;

        return res;
    }

    /** */
    private IgniteRel split(IgniteRel input) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = input.getTraitSet();

        Fragment fragment = new Fragment(new IgniteSender(cluster, traits, input));

        fragments.add(fragment);

        return new IgniteReceiver(cluster, traits, input.getRowType(), fragment);
    }

    /** */
    private void checkDistributionEqual(RelNode rel1, RelNode rel2) {
        if (!U.assertionsEnabled())
            return;

        IgniteDistribution distr1 = IgniteMdDistribution._distribution(rel1, mq);
        IgniteDistribution distr2 = IgniteMdDistribution._distribution(rel2, mq);

        assert Objects.equals(distr1, distr2) : "distr1=" + distr1 + "; distr2=" + distr2;
    }
}
