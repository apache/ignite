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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.engine.Constants;
import org.h2.result.Row;
import org.h2.value.Value;

/**
 * Single value row.
 */
public class H2PlainRowSingle extends H2Row {
    /** */
    private Value v;

    /**
     * @param v Value.
     */
    public H2PlainRowSingle(Value v) {
        this.v = v;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int idx) {
        assert idx == 0 : idx;

        return v;
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        assert idx == 0 : idx;

        this.v = v;
    }

    /** {@inheritDoc} */
    @Override public boolean hasSharedData(Row other) {
        if (other.getClass() == H2PlainRowSingle.class)
            return v == ((H2PlainRowSingle) other).v;

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int getMemory() {
        return Constants.MEMORY_OBJECT + (v == null ? 0 : v.getMemory());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2PlainRowSingle.class, this);
    }
}
