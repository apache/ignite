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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * Cursor.
 */
public class H2Cursor implements Cursor {
    /** */
    private final GridCursor<H2Row> cursor;

    /** */
    private final long time = U.currentTimeMillis();

    /**
     * @param cursor Cursor.
     */
    public H2Cursor(GridCursor<H2Row> cursor) {
        assert cursor != null;

        this.cursor = cursor;
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
                H2Row row = cursor.get();

                if (row.expireTime() > 0 && row.expireTime() <= time)
                    continue;

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
