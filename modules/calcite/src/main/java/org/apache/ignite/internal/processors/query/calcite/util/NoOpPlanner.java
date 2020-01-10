/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.util;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.CancelFlag;

/**
 *
 */
public final class NoOpPlanner implements RelOptPlanner {
    /** */
    public static final RelOptPlanner INSTANCE = new NoOpPlanner();

    /** */
    private NoOpPlanner() {}

    /** {@inheritDoc} */
    @Override public void setRoot(RelNode rel) {

    }

    /** {@inheritDoc} */
    @Override public RelNode getRoot() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean addRelTraitDef(RelTraitDef relTraitDef) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void clearRelTraitDefs() {

    }

    /** {@inheritDoc} */
    @Override public List<RelTraitDef> getRelTraitDefs() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public void clear() {

    }

    /** {@inheritDoc} */
    @Override public List<RelOptRule> getRules() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public boolean addRule(RelOptRule rule) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removeRule(RelOptRule rule) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Context getContext() {
        return Contexts.empty();
    }

    /** {@inheritDoc} */
    @Override public void setRuleDescExclusionFilter(Pattern exclusionFilter) {

    }

    /** {@inheritDoc} */
    @Override public void setCancelFlag(CancelFlag cancelFlag) {

    }

    /** {@inheritDoc} */
    @Override public RelNode changeTraits(RelNode rel, RelTraitSet toTraits) {
        return rel;
    }

    /** {@inheritDoc} */
    @Override public RelOptPlanner chooseDelegate() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void addMaterialization(RelOptMaterialization materialization) {

    }

    /** {@inheritDoc} */
    @Override public List<RelOptMaterialization> getMaterializations() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public void addLattice(RelOptLattice lattice) {

    }

    /** {@inheritDoc} */
    @Override public RelOptLattice getLattice(RelOptTable table) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public RelNode findBestExp() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public RelOptCostFactory getCostFactory() {
        return RelOptCostImpl.FACTORY;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
        return RelOptCostImpl.FACTORY.makeInfiniteCost();
    }

    /** {@inheritDoc} */
    @Override public RelOptCost getCost(RelNode rel) {
        return RelOptCostImpl.FACTORY.makeInfiniteCost();
    }

    /** {@inheritDoc} */
    @Override public RelNode register(RelNode rel, RelNode equivRel) {
        return rel;
    }

    /** {@inheritDoc} */
    @Override public RelNode ensureRegistered(RelNode rel, RelNode equivRel) {
        return rel;
    }

    /** {@inheritDoc} */
    @Override public boolean isRegistered(RelNode rel) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void registerSchema(RelOptSchema schema) {

    }

    /** {@inheritDoc} */
    @Override public void addListener(RelOptListener newListener) {

    }

    /** {@inheritDoc} */
    @Override public void registerMetadataProviders(List<RelMetadataProvider> list) {

    }

    /** {@inheritDoc} */
    @Override public long getRelMetadataTimestamp(RelNode rel) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void setImportance(RelNode rel, double importance) {

    }

    /** {@inheritDoc} */
    @Override public void registerClass(RelNode node) {

    }

    /** {@inheritDoc} */
    @Override public RelTraitSet emptyTraitSet() {
        return RelTraitSet.createEmpty();
    }

    /** {@inheritDoc} */
    @Override public void setExecutor(RexExecutor executor) {

    }

    /** {@inheritDoc} */
    @Override public RexExecutor getExecutor() {
        return RexUtil.EXECUTOR;
    }

    /** {@inheritDoc} */
    @Override public void onCopy(RelNode rel, RelNode newRel) {

    }
}
