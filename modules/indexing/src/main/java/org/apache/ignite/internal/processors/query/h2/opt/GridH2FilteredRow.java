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

import org.h2.value.Value;

/**
 * Row which was filtered out.
 */
public class GridH2FilteredRow extends GridH2Row {
    /** Partition ID. */
    private final int partId;

    /**
     * Constructor.
     *
     * @param partId Partition.
     */
    public GridH2FilteredRow(int partId) {
        super(null);

        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int index) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setValue(int index, Value v) {
        throw new UnsupportedOperationException();
    }
}
