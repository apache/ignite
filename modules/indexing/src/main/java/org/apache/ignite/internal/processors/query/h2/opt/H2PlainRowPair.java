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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.value.Value;

/**
 * Row of two values.
 */
public class H2PlainRowPair extends H2Row {
    /** */
    private Value v1;

    /** */
    private Value v2;

    /**
     * @param v1 First value.
     * @param v2 Second value.
     */
    public H2PlainRowPair(Value v1, Value v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int idx) {
        return idx == 0 ? v1 : v2;
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        if (idx == 0)
            v1 = v;
        else {
            assert idx == 1 : idx;

            v2 = v;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2PlainRowPair.class, this);
    }
}
