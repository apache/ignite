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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import java.util.Iterator;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;

/**
 *
 */
public final class CursorIteratorWrapper implements Iterator<H2Row> {
    /** */
    private final H2Cursor cursor;

    /** Next element. */
    private H2Row next;

    /**
     * @param cursor Cursor.
     */
    public CursorIteratorWrapper(H2Cursor cursor) {
        assert cursor != null;

        this.cursor = cursor;

        if (cursor.next())
            next = (H2Row)cursor.get();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return next != null;
    }

    /** {@inheritDoc} */
    @Override public H2Row next() {
        H2Row res = next;

        if (cursor.next())
            next = (H2Row)cursor.get();
        else
            next = null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException("operation is not supported");
    }
}
