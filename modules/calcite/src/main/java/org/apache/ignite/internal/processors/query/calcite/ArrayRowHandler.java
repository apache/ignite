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

package org.apache.ignite.internal.processors.query.calcite;

import org.apache.ignite.internal.processors.query.calcite.exec.EndMarker;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Handler for rows that implemented as a simple objects array.
 */
public class ArrayRowHandler implements RowHandler<Object[]> {
    /** */
    public static final RowHandler<Object[]> INSTANCE = new ArrayRowHandler();

    /** */
    private ArrayRowHandler() {}

    /** {@inheritDoc} */
    @Override public Object[] create(Object... fields) {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public Object get(int field, Object[] row) {
        return row[field];
    }

    /** {@inheritDoc} */
    @Override public void set(int field, Object[] row, Object val) {
        row[field] = val;
    }

    /** {@inheritDoc} */
    @Override public Object[] concat(Object[] left, Object[] right) {
        return F.concat(left, right);
    }

    /** {@inheritDoc} */
    @Override public Object[] endMarker() {
        return new Object[] {EndMarker.INSTANCE};
    }

    /** {@inheritDoc} */
    @Override public boolean isEndMarker(Object[] row) {
        return row[0] == EndMarker.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public int columnCount(Object[] row) {
        return row.length;
    }
}
