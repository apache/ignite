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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.Table;

/**
 * H2 {@link Index} adapter base class.
 */
public abstract class AbstractReduceIndexAdapter extends BaseIndex {
    /**
     * @param ctx  Context.
     * @param tbl  Table.
     * @param name Index name.
     * @param type Type.
     * @param cols Columns.
     */
    protected AbstractReduceIndexAdapter(GridKernalContext ctx,
        Table tbl,
        String name,
        IndexType type,
        IndexColumn[] cols
    ) {
        initBaseIndex(tbl, 0, name, cols, type);
    }

    /**
     * @return Index reducer.
     */
    abstract AbstractReducer reducer();

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        Cursor c = find(ses, null, null);

        long cnt = 0;

        while (c.next())
            cnt++;

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000;
    }

    /** {@inheritDoc} */
    @Override public final Cursor find(Session ses, SearchRow first, SearchRow last) {
        return reducer().find(first, last);
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw DbException.getUnsupportedException("findFirstOrLast");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }
}
