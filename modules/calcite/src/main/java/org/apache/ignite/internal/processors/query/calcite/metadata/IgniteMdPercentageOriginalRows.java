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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

/**
 * See {@link org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows}
 */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdPercentageOriginalRows implements MetadataHandler<BuiltInMetadata.PercentageOriginalRows> {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method, new IgniteMdPercentageOriginalRows());

    /** {@inheritDoc} */
    @Override public MetadataDef<BuiltInMetadata.PercentageOriginalRows> getDef() {
        return BuiltInMetadata.PercentageOriginalRows.DEF;
    }

    /** */
    public Double getPercentageOriginalRows(Aggregate rel, RelMetadataQuery mq) {
        // REVIEW jvs 28-Mar-2006: The assumption here seems to be that
        // aggregation does not apply any filtering, so it does not modify the
        // percentage.  That's very much oversimplified.
        return mq.getPercentageOriginalRows(rel.getInput());
    }

    /** */
    public Double getPercentageOriginalRows(Union rel, RelMetadataQuery mq) {
        double numerator = 0.0;
        double denominator = 0.0;

        // Ignore rel.isDistinct() because it's the same as an aggregate.

        // REVIEW jvs 28-Mar-2006: The original Broadbase formula was broken.
        // It was multiplying percentage into the numerator term rather than
        // than dividing it out of the denominator term, which would be OK if
        // there weren't summation going on.  Probably the cause of the error
        // was the desire to avoid division by zero, which I don't know how to
        // handle so I punt, meaning we return a totally wrong answer in the
        // case where a huge table has been completely filtered away.

        for (RelNode input : rel.getInputs()) {
            Double rowCount = mq.getRowCount(input);
            if (rowCount == null) {
                continue;
            }
            Double percentage = mq.getPercentageOriginalRows(input);
            if (percentage == null) {
                continue;
            }
            if (percentage != 0.0) {
                denominator += rowCount / percentage;
                numerator += rowCount;
            }
        }

        return quotientForPercentage(numerator, denominator);
    }

    /** */
    public Double getPercentageOriginalRows(Join rel, RelMetadataQuery mq) {
        // Assume any single-table filter conditions have already
        // been pushed down.

        // REVIEW jvs 28-Mar-2006: As with aggregation, this is
        // oversimplified.

        // REVIEW jvs 28-Mar-2006:  need any special casing for SemiJoin?

        Double left = mq.getPercentageOriginalRows(rel.getLeft());
        if (left == null) {
            return null;
        }
        Double right = mq.getPercentageOriginalRows(rel.getRight());
        if (right == null) {
            return null;
        }
        return left * right;
    }

    /** */
    public Double getPercentageOriginalRows(RelNode rel, RelMetadataQuery mq) {
        if (rel.getInputs().size() > 1) {
            // No generic formula available for multiple inputs.
            return null;
        }

        if (rel.getInputs().size() == 0) {
            // Assume no filtering happening at leaf.
            return 1.0;
        }

        RelNode child = rel.getInputs().get(0);

        Double childPercentage = mq.getPercentageOriginalRows(child);
        if (childPercentage == null) {
            return null;
        }

        // Compute product of percentage filtering from this rel (assuming any
        // filtering is the effect of single-table filters) with the percentage
        // filtering performed by the child.
        Double relPercentage =
            quotientForPercentage(mq.getRowCount(rel), mq.getRowCount(child));
        if (relPercentage == null) {
            return null;
        }
        double percent = relPercentage * childPercentage;

        // this check is needed in cases where this method is called on a
        // physical rel
        if ((percent < 0.0) || (percent > 1.0)) {
            return null;
        }
        return relPercentage * childPercentage;
    }

    /** */
    private static Double quotientForPercentage(
        Double numerator,
        Double denominator) {
        if ((numerator == null) || (denominator == null)) {
            return null;
        }

        // may need epsilon instead
        if (denominator == 0.0) {
            // cap at 100%
            return 1.0;
        }
        else {
            return numerator / denominator;
        }
    }
}
