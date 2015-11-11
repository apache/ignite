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

import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.value.Value;

/**
 * Row with locking support needed for unique key conflicts resolution.
 */
public abstract class GridH2Row extends Row implements GridSearchRowPointer {
    /** {@inheritDoc} */
    @Override public long pointer() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void incrementRefCount() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void decrementRefCount() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public Row getCopy() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setVersion(int version) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getByteCount(Data dummy) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setDeleted(boolean deleted) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setSessionId(int sessionId) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getSessionId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void commit() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isDeleted() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setKeyAndVersion(SearchRow old) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getVersion() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setKey(long key) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long getKey() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getMemory() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Value[] getValueList() {
        throw new UnsupportedOperationException();
    }
}