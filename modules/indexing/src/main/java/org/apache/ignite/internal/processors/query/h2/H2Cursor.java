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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.query.h2.opt.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.h2.index.*;
import org.h2.message.*;
import org.h2.result.*;

/**
 * Cursor.
 */
public class H2Cursor implements Cursor {
    /** */
    private final GridCursor<GridH2Row> cursor;

    /** */
    private final IgniteBiPredicate<Object,Object> filter;

    /** */
    private final long time = U.currentTimeMillis();

    /**
     * @param cursor Cursor.
     * @param filter Filter.
     */
    public H2Cursor(GridCursor<GridH2Row> cursor, IgniteBiPredicate<Object, Object> filter) {
        assert cursor != null;

        this.cursor = cursor;
        this.filter = filter;
    }

    /**
     * @param cursor Cursor.
     */
    public H2Cursor(GridCursor<GridH2Row> cursor) {
        this(cursor, null);
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        try {
            return cursor.get();
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        try {
            while (cursor.next()) {
                GridH2Row row = cursor.get();

                if (row.expireTime() > 0 && row.expireTime() <= time)
                    continue;

                if (filter == null)
                    return true;

                Object key = row.getValue(0).getObject();
                Object val = row.getValue(1).getObject();

                assert key != null;
                assert val != null;

                if (filter.apply(key, val))
                    return true;
            }

            return false;
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        throw DbException.getUnsupportedException("previous");
    }
}
