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
    T visit(IgniteJoin rel);

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
    T visit(IgniteAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteMapAggregate rel);

    /**
     * See {@link IgniteRelVisitor#visit(IgniteRel)}
     */
    T visit(IgniteReduceAggregate rel);

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
     * Visits a relational node and calculates a result on the basis of node meta information.
     * @param rel Relational node.
     * @return Visit result.
     */
    T visit(IgniteRel rel);
}
