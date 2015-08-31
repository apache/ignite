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

import java.util.Iterator;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * H2 Cursor implementation.
 */
public class GridH2Cursor implements Cursor {
    /** */
    private Iterator<GridH2Row> iter;

    /** */
    private Row row;

    /**
     * Constructor.
     *
     * @param iter Rows iterator.
     */
    public GridH2Cursor(Iterator<GridH2Row> iter) {
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        return row;
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        row = null;

        if (iter.hasNext())
            row = iter.next();

        return row != null;
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        // Should never be called.
        throw DbException.getUnsupportedException("previous");
    }
}