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

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Update plan - where to take data to update cache from and how to construct new keys and values, if needed.
 */
public class UpdatePlan {
    /** Initial statement to drive the rest of the logic. */
    public final UpdateMode mode;

    /** Target table to be affected by initial DML statement. */
    public final GridH2Table tbl;

    /** Column names to set or update. */
    public final String[] colNames;

    /** Method to create key for INSERT or MERGE, ignored for UPDATE and DELETE. */
    public final Supplier keySupplier;

    /** Method to create value to put to cache, ignored for DELETE. */
    public final Supplier valSupplier;

    /** Index of key column, if it's explicitly mentioned in column list of MERGE or INSERT,
     * ignored for UPDATE and DELETE. */
    public final int keyColIdx;

    /** Index of value column, if it's explicitly mentioned in column list. Ignored for UPDATE and DELETE. */
    public final int valColIdx;

    /** SELECT statement built upon initial DML statement. */
    public final String selectQry;

    /** Subquery flag - {@code true} if {@link #selectQry} is a two-step subquery, {@code false} otherwise. */
    public final boolean isSubqry;

    /** Number of rows in rows based MERGE or INSERT. */
    public final int rowsNum;

    /** Entry filter for fast UPDATE or DELETE. */
    public final GridTriple<FastUpdateOperand> singleUpdate;

    /** */
    private UpdatePlan(UpdateMode mode, GridH2Table tbl, String[] colNames, Supplier keySupplier,
               Supplier valSupplier, int keyColIdx, int valColIdx, String selectQry, boolean isSubqry,
               int rowsNum, GridTriple<FastUpdateOperand> singleUpdate) {
        this.colNames = colNames;
        this.rowsNum = rowsNum;
        assert mode != null;
        assert tbl != null;

        this.mode = mode;
        this.tbl = tbl;
        this.keySupplier = keySupplier;
        this.valSupplier = valSupplier;
        this.keyColIdx = keyColIdx;
        this.valColIdx = valColIdx;
        this.selectQry = selectQry;
        this.isSubqry = isSubqry;
        this.singleUpdate = singleUpdate;
    }

    /** */
    public static UpdatePlan forMerge(GridH2Table tbl, String[] colNames, Supplier keySupplier,
                                       Supplier valSupplier, int keyColIdx, int valColIdx, String selectQry, boolean isSubqry, int rowsNum) {
        assert !F.isEmpty(colNames);

        return new UpdatePlan(UpdateMode.MERGE, tbl, colNames, keySupplier, valSupplier, keyColIdx, valColIdx,
            selectQry, isSubqry, rowsNum, null);
    }

    /** */
    public static UpdatePlan forInsert(GridH2Table tbl, String[] colNames, Supplier keySupplier,
                                        Supplier valSupplier, int keyColIdx, int valColIdx, String selectQry, boolean isSubqry, int rowsNum) {
        assert !F.isEmpty(colNames);

        return new UpdatePlan(UpdateMode.INSERT, tbl, colNames, keySupplier, valSupplier, keyColIdx, valColIdx, selectQry,
            isSubqry, rowsNum, null);
    }

    /** */
    public static UpdatePlan forUpdate(GridH2Table tbl, String[] colNames, Supplier valSupplier,
                                        int valColIdx, String selectQry) {
        assert !F.isEmpty(colNames);

        return new UpdatePlan(UpdateMode.UPDATE, tbl, colNames, null, valSupplier, -1, valColIdx, selectQry,
            true, 0, null);
    }

    /** */
    public static UpdatePlan forDelete(GridH2Table tbl, String selectQry) {
        return new UpdatePlan(UpdateMode.DELETE, tbl, null, null, null, -1, -1, selectQry, true, 0, null);
    }

    /** */
    public static UpdatePlan forFastUpdate(UpdateMode mode, GridH2Table tbl, GridTriple<FastUpdateOperand> singleUpdate) {
        assert mode == UpdateMode.UPDATE || mode == UpdateMode.DELETE;

        return new UpdatePlan(mode, tbl, null, null, null, -1, -1, null, false, 0, singleUpdate);
    }

    /**
     * DML statement execution plan type - MERGE/INSERT from rows or subquery,
     * or UPDATE/DELETE from subquery or literals/params based.
     */
    public enum UpdateMode {
        /** */
        MERGE,

        /** */
        INSERT,

        /** */
        UPDATE,

        /** */
        DELETE,
    }
}
