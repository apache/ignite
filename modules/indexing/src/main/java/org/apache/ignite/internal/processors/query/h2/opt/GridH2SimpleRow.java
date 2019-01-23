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

import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersionAware;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionAware;
import org.h2.value.Value;
import org.h2.value.ValueString;

/**
 *
 */
public class GridH2SimpleRow extends GridH2Row {
    /** Special value for not initialized fields. */
    public static final Value NOT_INITIALIZED = new ValueString(null) {};

    /** Row values. */
    private final Value[] data;

    /**
     * @param colCnt Columns count.
     */
    public GridH2SimpleRow(int colCnt) {
        super(null);

        data = new Value[colCnt];

        Arrays.fill(data, NOT_INITIALIZED);

        System.out.println("+++ GridH2SimpleRow");
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return data.length;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int index) {
        assert data[index] != NOT_INITIALIZED : "Access to not gathered field: " + index;

        return data[index];
    }

    /** {@inheritDoc} */
    @Override public void setValue(int index, Value v) {
        data[index] = v;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return 0;
    }
    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void newMvccVersion(MvccUpdateVersionAware other) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void newMvccVersion(MvccVersion ver) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void newMvccVersion(long crd, long cntr, int opCntr) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public MvccVersion newMvccVersion() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void mvccVersion(MvccVersionAware other) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void mvccVersion(MvccVersion ver) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void mvccVersion(long crd, long cntr, int opCntr) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public MvccVersion mvccVersion() {
        throw new UnsupportedOperationException();
    }
}