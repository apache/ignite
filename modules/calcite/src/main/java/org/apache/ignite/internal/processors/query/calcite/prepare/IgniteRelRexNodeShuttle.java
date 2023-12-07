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

import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;

/** */
public class IgniteRelRexNodeShuttle extends IgniteRelShuttle {
    /** */
    private final RexShuttle rexShuttle;

    /** */
    public IgniteRelRexNodeShuttle(RexShuttle rexShuttle) {
        this.rexShuttle = rexShuttle;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        rexShuttle.apply(rel.getCondition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        rexShuttle.apply(rel.getProjects());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteNestedLoopJoin rel) {
        rexShuttle.apply(rel.getCondition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteCorrelatedNestedLoopJoin rel) {
        rexShuttle.apply(rel.getCondition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMergeJoin rel) {
        rexShuttle.apply(rel.getCondition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        rexShuttle.apply(rel.projects());
        rexShuttle.apply(rel.condition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        rexShuttle.apply(rel.projects());
        rexShuttle.apply(rel.condition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSortedIndexSpool rel) {
        rexShuttle.apply(rel.condition());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteHashIndexSpool rel) {
        rexShuttle.apply(rel.condition());
        rexShuttle.apply(rel.searchRow());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        rexShuttle.apply(rel.getSourceExpressionList());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteLimit rel) {
        rexShuttle.apply(rel.fetch());
        rexShuttle.apply(rel.offset());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSort rel) {
        rexShuttle.apply(rel.offset);
        rexShuttle.apply(rel.fetch);
        rexShuttle.apply(rel.getSortExps());

        return super.visit(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableFunctionScan rel) {
        rexShuttle.apply(rel.getCall());

        return super.visit(rel);
    }
}
