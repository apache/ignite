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

import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdMinRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import static org.apache.calcite.rel.metadata.RelMdUtil.literalValueApproximatedByDouble;

/** Minimum row count metadata compatible with expression-based FETCH and OFFSET. */
// TODO: https://issues.apache.org/jira/browse/CALCITE-7592
//  Remove this class and its registration in IgniteMetadata after upgrading to Calcite 1.43.
@SuppressWarnings("unused") // Actually all methods are used by runtime generated classes.
public class IgniteMdMinRowCount extends RelMdMinRowCount {
    /** Metadata provider. */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.MIN_ROW_COUNT.method, new IgniteMdMinRowCount());

    /** {@inheritDoc} */
    @Override public Double getMinRowCount(Sort rel, RelMetadataQuery mq) {
        Double rowCnt = mq.getMinRowCount(rel.getInput());

        if (rowCnt == null)
            rowCnt = 0D;

        double offset = literalValueApproximatedByDouble(rel.offset,
            rel.offset == null ? 0D : rowCnt);

        rowCnt = Math.max(rowCnt - offset, 0D);

        double limit = literalValueApproximatedByDouble(rel.fetch,
            rel.fetch == null ? rowCnt : 0D);

        return limit < rowCnt ? limit : rowCnt;
    }
}
