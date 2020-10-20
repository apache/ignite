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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;

/** */
public class RelRegistrar extends AbstractRelNode implements IgniteRel {
    /** */
    private final List<RelNode> rels;

    /** */
    private final RelNode orig;

    /** */
    public RelRegistrar(RelOptCluster cluster, RelTraitSet traitSet, RelNode orig, List<RelNode> rels) {
        super(cluster, traitSet);

        this.rels = rels;
        this.orig = orig;
    }

    /** {@inheritDoc} */
    @Override protected RelDataType deriveRowType() {
        return orig.getRowType();
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return pw.item("orig", orig).item("requiredTraits", getTraitSet());
    }

    /** {@inheritDoc} */
    @Override public RelNode onRegister(RelOptPlanner planner) {
        RelNode r = null;
        for (RelNode rel : rels) {
            r = planner.ensureRegistered(rel, orig);
            assert r == rel || RelOptUtil.equal("rowtype of rel before registration",
                rel.getRowType(),
                "rowtype of rel after registration",
                r.getRowType(),
                Litmus.THROW);
            if (!r.getTraitSet().satisfies(getTraitSet()))
                r = RelOptRule.convert(r, getTraitSet());
        }
        assert r != null && r.isValid(Litmus.THROW, null);
        return r;
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new RelRegistrar(getCluster(), traitSet, orig, rels);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeInfiniteCost();
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        throw new AssertionError();
    }
}
