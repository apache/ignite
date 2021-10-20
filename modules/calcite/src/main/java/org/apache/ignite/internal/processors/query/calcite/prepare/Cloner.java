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
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSetOp;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.util.ArrayUtils.asList;

/** */
public class Cloner implements IgniteRelVisitor<IgniteRel> {
    /** */
    private final RelOptCluster cluster;

    /** */
    private List<IgniteReceiver> remotes;

    /** */
    Cloner(RelOptCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Clones and associates a plan with a new cluster.
     *
     * @param src Fragment to clone.
     * @return New plan.
     */
    public Fragment go(Fragment src) {
        try {
            remotes = new ArrayList<>();

            IgniteRel newRoot = visit(src.root());

            return new Fragment(src.fragmentId(), newRoot, List.copyOf(remotes), src.serialized(), src.mapping());
        }
        finally {
            remotes = null;
        }
    }

    /** */
    public static IgniteRel clone(IgniteRel r) {
        Cloner c = new Cloner(r.getCluster());

        return c.visit(r);
    }

    /** */
    private IgniteReceiver collect(IgniteReceiver receiver) {
        if (remotes != null)
            remotes.add(receiver);

        return receiver;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSender rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTrimExchange rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteNestedLoopJoin rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getLeft()),
            visit((IgniteRel) rel.getRight())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteCorrelatedNestedLoopJoin rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getLeft()),
            visit((IgniteRel) rel.getRight())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMergeJoin rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getLeft()),
            visit((IgniteRel) rel.getRight())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        return rel.clone(cluster, asList());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return rel.clone(cluster, asList());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
        return rel.clone(cluster, asList());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteUnionAll rel) {
        return rel.clone(cluster, Commons.transform(rel.getInputs(), rel0 -> visit((IgniteRel) rel0)));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSort rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableSpool rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSortedIndexSpool rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteLimit rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        return collect((IgniteReceiver)rel.clone(cluster, asList()));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSingleHashAggregate rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMapHashAggregate rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReduceHashAggregate rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSingleSortAggregate rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMapSortAggregate rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReduceSortAggregate rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteHashIndexSpool rel) {
        return rel.clone(cluster, asList(visit((IgniteRel) rel.getInput())));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSetOp rel) {
        return rel.clone(cluster, Commons.transform(rel.getInputs(), rel0 -> visit((IgniteRel) rel0)));
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableFunctionScan rel) {
        return rel.clone(cluster, asList());
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteRel rel) {
        return rel.accept(this);
    }
}
