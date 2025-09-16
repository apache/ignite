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

package org.apache.ignite.internal.processors.query.calcite.rel;

import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSetOp;

/**
 * A visitor to traverse an Ignite relational nodes tree.
 */
public interface IgniteRelVisitor<T> {
    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteSender rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteFilter rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteTrimExchange rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteProject rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteNestedLoopJoin rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteCorrelatedNestedLoopJoin rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteMergeJoin rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteIndexScan rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteIndexCount rel);


    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteIndexBound rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteTableScan rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteReceiver rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteExchange rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteColocatedHashAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteMapHashAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteReduceHashAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteColocatedSortAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteMapSortAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteReduceSortAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteTableModify rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteValues rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteUnionAll rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteSort rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteTableSpool rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteSortedIndexSpool rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteLimit rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteHashIndexSpool rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteSetOp rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteTableFunctionScan rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteCollect rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteUncollect rel);

    /**
     * Visits a relational node and calculates a result on the basis of node meta information.
     * @param rel Relational node.
     * @return Visit result.
     */
    T visit(IgniteRel rel);
}
