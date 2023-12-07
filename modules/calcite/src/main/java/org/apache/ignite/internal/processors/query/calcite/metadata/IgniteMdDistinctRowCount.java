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

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdDistinctRowCount extends RelMdDistinctRowCount {
    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.DISTINCT_ROW_COUNT.method, new IgniteMdDistinctRowCount());

    /** {@inheritDoc} */
    @Override public Double getDistinctRowCount(
        RelSubset rel,
        RelMetadataQuery mq,
        ImmutableBitSet groupKey,
        RexNode predicate
    ) {
        if (groupKey.cardinality() == 0)
            return 1d;

        double rowCount = mq.getRowCount(rel);

        rowCount *= 1.0 - Math.pow(.5, groupKey.cardinality());

        return rowCount;
    }
}
