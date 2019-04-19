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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * Simple cursor from a single node.
 */
public class UnicastCursor implements Cursor {
    /** */
    private final int rangeId;

    /** */
    private final RangeStream stream;

    /**
     * @param rangeId Range ID.
     * @param stream Stream.
     */
    public UnicastCursor(int rangeId, RangeStream stream) {
        assert stream != null;

        this.rangeId = rangeId;
        this.stream = stream;
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        return stream.next(rangeId);
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        return stream.get(rangeId);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getSearchRow() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean previous() {
        throw new UnsupportedOperationException();
    }
}
