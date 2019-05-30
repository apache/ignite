/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.opt;

/**
 * Table statistics class. Used by query optimizer to estimate execution plan cost.
 */
public class TableStatistics {
    /** Total table row count (including primary and backup partitions). */
    private final long totalRowCnt;

    /** Primary parts row count. */
    private final long primaryRowCnt;

    /**
     * @param totalRowCnt Total table row count (including primary and backup partitions).
     * @param primaryRowCnt Primary parts row count.
     */
    public TableStatistics(long totalRowCnt, long primaryRowCnt) {
        assert totalRowCnt >= 0 && primaryRowCnt >= 0;

        this.totalRowCnt = totalRowCnt;
        this.primaryRowCnt = primaryRowCnt;
    }

    /**
     * @return Total table row count (including primary and backup partitions).
     */
    public long totalRowCount() {
        return totalRowCnt;
    }

    /**
     * @return Primary parts row count.
     */
    public long primaryRowCount() {
        return primaryRowCnt;
    }
}
