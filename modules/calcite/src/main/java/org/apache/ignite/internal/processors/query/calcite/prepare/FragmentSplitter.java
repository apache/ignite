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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
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
        assert cutPoint != null;
        assert rel != cutPoint;

        IgniteRel input = (IgniteRel) rel.getInput();
        IgniteRel newInput = visit(input);

        assert input != newInput;

        checkTraitsEqual(input, newInput);

        RelTraitSet traits = rel.getTraitSet();

        return (IgniteRel) rel.copy(traits, F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        if (cutPoint == rel)
            return split(rel);

        IgniteRel input = (IgniteRel) rel.getInput();
        IgniteRel newInput = visit(input);

        if (input == newInput)
            return rel;

        checkTraits(input, newInput);

        RelTraitSet traits = rel.getTraitSet()
            .replace(newInput.distribution());

        return (IgniteRel) rel.copy(traits, F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTrimExchange rel) {
        if (cutPoint == rel)
            return split(rel);

        if (cutPoint == null)
            return (IgniteRel) rel.getInput();

        IgniteRel input = (IgniteRel) rel.getInput();
        IgniteRel newInput = visit(input);

        if (input == newInput)
            return rel;

        checkTraitsEqual(input, newInput);

        RelTraitSet traits = rel.getTraitSet();

        return (IgniteRel) rel.copy(traits, F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        if (cutPoint == rel)
            return split(rel);

        IgniteRel input = (IgniteRel) rel.getInput();
        IgniteRel newInput = visit(input);

        if (input == newInput)
            return rel;

        checkTraits(input, newInput);

        RelTraitSet traits = rel.getTraitSet()
            .replace(IgniteDistributions.project(mq, newInput, rel.getProjects()));

        return (IgniteRel) rel.copy(traits, F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteJoin rel) {
        if (cutPoint == null)
            return rel; // No need to check deeper

        if (cutPoint == rel)
            return split(rel);

        IgniteRel left = (IgniteRel) rel.getLeft();
        IgniteRel newLeft = visit(left);
        IgniteRel right = (IgniteRel) rel.getRight();
        IgniteRel newRight = visit(right);

        if (left == newLeft && right == newRight)
            return rel;

        checkTraitsEqual(left, newLeft);
        checkTraitsEqual(right, newRight);

        RelTraitSet traits = rel.getTraitSet();

        return (IgniteRel) rel.copy(traits, F.asList(newLeft, newRight));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        if (rel == cutPoint)
            return split(rel);

        assert cutPoint == null;

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        assert cutPoint == null;

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteAggregate rel) {
        if (rel == cutPoint)
            return split(rel);

        IgniteRel input = (IgniteRel) rel.getInput();
        IgniteRel newInput = visit(input);

        if (input == newInput)
            return rel;

        checkTraits(input, newInput);

        IgniteDistribution distr = IgniteDistributions.aggregate(mq,
            newInput, rel.getGroupSet(), rel.getGroupSets(), rel.getAggCallList());

        assert distr != null;

        RelTraitSet traits = rel.getTraitSet().replace(distr);

        return (IgniteRel) rel.copy(traits, F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMapAggregate rel) {
        assert rel != cutPoint;

        IgniteRel input = (IgniteRel) rel.getInput();
        IgniteRel newInput = visit(input);

        if (input == newInput)
            return rel;

        checkTraitsEqual(input, newInput);

        RelTraitSet traits = rel.getTraitSet();

        return (IgniteRel) rel.copy(traits, F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReduceAggregate rel) {
        assert cutPoint == null;

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        if (cutPoint == null)
            return rel; // No need to check deeper

        if (rel == cutPoint)
            return split(rel);

        IgniteRel input = (IgniteRel) rel.getInput();
        IgniteRel newInput = visit(input);

        if (input == newInput)
            return rel;

        checkTraitsEqual(input, newInput);

        RelTraitSet traits = rel.getTraitSet();

        return (IgniteRel) rel.copy(traits, F.asList(newInput));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
        assert cutPoint == null;

        return rel;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteUnionAll rel) {
        if (cutPoint == null)
            return rel; // No need to check deeper

        if (rel == cutPoint)
            return split(rel);

        boolean inputChanged = false;

        List<IgniteRel> inputs = Commons.cast(rel.getInputs());
        List<IgniteRel> newInputs = new ArrayList<>(inputs.size());

        for (RelNode input : inputs) {
            IgniteRel newInput = visit(input);

            if (input != newInput) {
                inputChanged = true;

                checkTraitsEqual((IgniteRel) input, newInput);
            }

            newInputs.add(newInput);
        }

        if (!inputChanged)
            return rel;

        return (IgniteRel) rel.copy(rel.getTraitSet(), Commons.cast(newInputs));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** */
    private IgniteRel visit(RelNode rel) {
        return visit((IgniteRel) rel);
    }

    /** */
    private void checkTraits(IgniteRel orig, IgniteRel newRel) {
        if (!U.assertionsEnabled() || orig == newRel)
            return;

        RelTraitSet origTraits = orig.getTraitSet();
        RelTraitSet newTraits = newRel.getTraitSet();

        ImmutableList<RelTrait> difference = origTraits.difference(newTraits);

        // Only distribution trait may be changed after split
        if (difference.isEmpty() || difference.size() == 1 && F.first(difference) instanceof DistributionTrait)
            return;

        assert false : difference;
    }

    /** */
    private void checkTraitsEqual(IgniteRel orig, IgniteRel newRel) {
        if (!U.assertionsEnabled() || orig == newRel)
            return;

        RelTraitSet origTraits = orig.getTraitSet();
        RelTraitSet newTraits = newRel.getTraitSet();

        ImmutableList<RelTrait> difference = origTraits.difference(newTraits);

        if (difference.isEmpty())
            return;

        assert false : difference;
    }

    /** */
    private IgniteRel split(IgniteRel rel) {
        cutPoint = null;

        IgniteRel newRel = visit(rel);

        checkTraits(rel, newRel);

        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = rel.getTraitSet();

        IgniteSender sender = new IgniteSender(cluster, traits, newRel);

        Fragment fragment = new Fragment(sender);

        fragments.add(fragment);

        return new IgniteReceiver(cluster, traits, fragment);
    }
}
