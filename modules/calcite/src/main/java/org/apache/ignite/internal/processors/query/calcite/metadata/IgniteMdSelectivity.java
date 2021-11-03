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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

/**
 *
 */
public class IgniteMdSelectivity extends RelMdSelectivity {
    /**
     *
     */
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.SELECTIVITY.method, new IgniteMdSelectivity());

    /**
     *
     */
    public Double getSelectivity(AbstractIndexScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return getSelectivity((ProjectableFilterableTableScan) rel, mq, predicate);
        }

        List<RexNode> lowerCond = rel.lowerCondition();
        List<RexNode> upperCond = rel.upperCondition();

        if (nullOrEmpty(lowerCond) && nullOrEmpty(upperCond)) {
            return RelMdUtil.guessSelectivity(rel.condition());
        }

        double idxSelectivity = 1.0;
        int len = nullOrEmpty(lowerCond) ? upperCond.size() : nullOrEmpty(upperCond) ? lowerCond.size() :
                Math.max(lowerCond.size(), upperCond.size());

        for (int i = 0; i < len; i++) {
            RexCall lower = nullOrEmpty(lowerCond) || lowerCond.size() <= i ? null : (RexCall) lowerCond.get(i);
            RexCall upper = nullOrEmpty(upperCond) || upperCond.size() <= i ? null : (RexCall) upperCond.get(i);

            assert lower != null || upper != null;

            if (lower != null && upper != null) {
                idxSelectivity *= lower.op.kind == SqlKind.EQUALS ? .1 : .2;
            } else {
                idxSelectivity *= .35;
            }
        }

        List<RexNode> conjunctions = RelOptUtil.conjunctions(rel.condition());

        if (!nullOrEmpty(lowerCond)) {
            conjunctions.removeAll(lowerCond);
        }
        if (!nullOrEmpty(upperCond)) {
            conjunctions.removeAll(upperCond);
        }

        RexNode remaining = RexUtil.composeConjunction(RexUtils.builder(rel), conjunctions, true);

        return idxSelectivity * RelMdUtil.guessSelectivity(remaining);
    }

    /**
     *
     */
    public Double getSelectivity(ProjectableFilterableTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null) {
            return RelMdUtil.guessSelectivity(rel.condition());
        }

        RexNode condition = rel.pushUpPredicate();
        if (condition == null) {
            return RelMdUtil.guessSelectivity(predicate);
        }

        RexNode diff = RelMdUtil.minusPreds(RexUtils.builder(rel), predicate, condition);
        return RelMdUtil.guessSelectivity(diff);
    }

    /**
     *
     */
    public Double getSelectivity(IgniteSortedIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                    RelMdUtil.minusPreds(
                            rel.getCluster().getRexBuilder(),
                            predicate,
                            rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }

    /**
     *
     */
    public Double getSelectivity(IgniteHashIndexSpool rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate != null) {
            return mq.getSelectivity(rel.getInput(),
                    RelMdUtil.minusPreds(
                            rel.getCluster().getRexBuilder(),
                            predicate,
                            rel.condition()));
        }

        return mq.getSelectivity(rel.getInput(), rel.condition());
    }
}
