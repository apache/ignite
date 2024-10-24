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

import java.util.Collection;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.result.ResultInterface;
import org.h2.value.Value;

/**
 * Simple array based row.
 */
public class H2PlainRow extends H2Row {
    /** */
    private final int colCnt;

    /** */
    @GridToStringInclude
    private final Value[] vals;

    /**
     * @param colCnt Column count. H2 engine can add extra columns at the end of result set.
     * @param vals Values.
     * @see ResultInterface#getVisibleColumnCount()
     * @see GridH2ValueMessageFactory#toMessages(Collection, Collection, int)
     * @see ResultInterface#currentRow()
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public H2PlainRow(int colCnt, Value[] vals) {
        this.colCnt = colCnt;
        this.vals = vals;
    }

    /**
     * @param colCnt Length.
     */
    public H2PlainRow(int colCnt) {
        this.colCnt = colCnt;
        vals = new Value[colCnt];
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return colCnt;
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
    @Override public String toString() {
        return S.toString(H2PlainRow.class, this);
    }
}
