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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.engine.Constants;
import org.h2.result.Row;
import org.h2.value.Value;

/**
 * Simple array based row.
 */
public class H2PlainRow extends H2Row {
    /** */
    @GridToStringInclude
    private Value[] vals;

    /** Row size. */
    int memory = MEMORY_CALCULATE;

    /**
     * @param vals Values.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public H2PlainRow(Value[] vals) {
        this.vals = vals;
    }

    /**
     * @param len Length.
     */
    public H2PlainRow(int len) {
        vals = new Value[len];
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return vals.length;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int idx) {
        return vals[idx];
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        vals[idx] = v;
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean hasSharedData(Row other) {
        if (other.getClass() == H2PlainRow.class)
            return vals == ((H2PlainRow) other).vals;

        return false;
    }

    /** {@inheritDoc} */
    @Override public int getMemory() {
        if (memory != MEMORY_CALCULATE)
            return memory;

        int size = 24 /* H2PlainRow obj size. */;
        if (!F.isEmpty(vals)) {
            int len = vals.length;

            size += Constants.MEMORY_ARRAY + len * Constants.MEMORY_POINTER;

            for (Value v : vals) {
                if (v != null)
                    size += v.getMemory();
            }
        }

        memory = size;

        return memory;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2PlainRow.class, this);
    }
}
