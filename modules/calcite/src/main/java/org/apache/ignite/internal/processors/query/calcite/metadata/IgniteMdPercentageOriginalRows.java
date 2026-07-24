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

import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;

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

    /**
     * Estimates the percentage of the number of rows actually produced by a relational expression out of the number of
     * rows it would produce if all single-table filter conditions were removed.
     *
     * @return estimated percentage (between 0.0 and 1.0), or null if no reliable estimate can be determined
     */
    public Double getPercentageOriginalRows(ProjectableFilterableTableScan rel, RelMetadataQuery mq) {
        Double tableRowCnt = rel.getTable().getRowCount();
        Double relRowCnt = mq.getRowCount(rel);

        if (tableRowCnt == null || relRowCnt == null)
            return null;

        return quotientForPercentage(relRowCnt, tableRowCnt);
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
