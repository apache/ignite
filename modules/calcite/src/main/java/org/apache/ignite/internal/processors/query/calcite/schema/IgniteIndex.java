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
package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.function.Predicate;
import org.apache.calcite.rel.RelCollation;
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.IndexScan;

/**
 * Ignite scannable index.
 */
public class IgniteIndex {
    /** */
    private final RelCollation collation;

    /** */
    private final String idxName;

    /** */
    private final GridIndex idx;

    /** */
    private final IgniteTable tbl;

    /** */
    public IgniteIndex(RelCollation collation, String name, GridIndex idx, IgniteTable tbl) {
        this.collation = collation;
        this.idxName = name;
        this.idx = idx;
        this.tbl = tbl;
    }

    /** */
    public RelCollation collation() {
        return collation;
    }

    /** */
    public String name() {
        return idxName;
    }

    /** */
    public GridIndex index() {
        return idx;
    }

    /** */
    public IgniteTable table() {
        return tbl;
    }

    /** */
    public Iterable<Object[]> scan(
        ExecutionContext execCtx,
        Predicate<Object[]> filters,
        Object[] lowerIdxConditions,
        Object[] upperIdxConditions) {
        return new IndexScan(execCtx, this, filters, lowerIdxConditions, upperIdxConditions);
    }
}
