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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.Set;

/**
 *
 */
public class GridH2UsedColumnInfo {
    /** Columns to extract from full row. */
    private final Set<Integer> colsToExtract;

    /** Columns count in full row. */
    private final int colsCnt;

    /**
     * @param colsToExtract Columns to extract from full row.
     * @param colsCnt Columns count in full row.
     */
    public GridH2UsedColumnInfo(Set<Integer> colsToExtract, int colsCnt) {
        this.colsToExtract = colsToExtract;
        this.colsCnt = colsCnt;
    }

    /**
     * @return Columns IDs to extract from full row.
     */
    public Set<Integer> columns() {
        return colsToExtract;
    }

    /**
     * @return Columns count in full row.
     */
    public int columnsCount() {
        return colsCnt;
    }
}