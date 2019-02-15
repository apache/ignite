/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.value.Value;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;

/**
 * Dummy H2 search row adadpter.
 */
public abstract class GridH2SearchRowAdapter implements GridH2SearchRow {
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
        // No-op, may be set in H2 INFORMATION_SCHEMA.
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
    @Override public Value[] getValueList() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return MVCC_CRD_COUNTER_NA;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return MVCC_COUNTER_NA;
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        return MVCC_OP_COUNTER_NA;
    }

    /** {@inheritDoc} */
    @Override public byte mvccTxState() {
        return TxState.NA;
    }
}
