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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPredicates;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;

/** */
public class IgniteMdPredicates extends RelMdPredicates {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
        .reflectiveSource(BuiltInMethod.PREDICATES.method, new IgniteMdPredicates());

    /**
     * See {@link RelMdPredicates#getPredicates(org.apache.calcite.rel.RelNode, org.apache.calcite.rel.metadata.RelMetadataQuery)}
     */
    public RelOptPredicateList getPredicates(IgniteIndexScan rel, RelMetadataQuery mq) {
        if (rel.condition() == null)
            return RelOptPredicateList.EMPTY;

        return RelOptPredicateList.of(rel.getCluster().getRexBuilder(),
                    RexUtil.retainDeterministic(
                        RelOptUtil.conjunctions(rel.condition())));
    }
}
