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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;

/**
 * A savepoint is a special mark inside a transaction that allows all commands
 * that are executed after it was established to be rolled back,
 * restoring the transaction state to what it was at the time of the savepoint.
 *
 * <h1 class="header">Usage</h1>
 *
 * The execution result guarantee that only values 1 and 3 are inserted into cache:
 *
 * <pre name="code" class="java">
 * Ignite ignite = ....;
 * IgniteCache<Integer, Integer> c = ....;
 *
 * try (Transaction tx = ignite.transactions().txStart()) {
 * c.put(1, 1);
 *
 * tx.savepoint("mysavepoint");
 *
 * c.put(2, 2);
 *
 * tx.rollbackToSavepoint("mysavepoint");
 *
 * c.put(3, 3);
 *
 * tx.commit();
 * }
 * </pre>
 *
 * The result of this transaction is:
 * <p>
 * cache.get(1) - 1,
 * <br>
 * cache.get(2) - null,
 * <br>
 * cache.get(3) - 3.
 * </p>
 * <h1 class="header">Restrictions</h1>
 *
 * <ul>
 * <li>Savepoints works only with {@code TRANSACTIONAL} caches on the node where transaction started.</li>
 * <li>In case of {@code ATOMIC} caches and caches from other nodes - they will be ignored.
 * There is no need in savepoints because every action commits immediately.</li>
 * </ul>
 */
class TxSavepoint {
    /** Savepoint ID. */
    private final String name;

    /** Per-transaction read map. */
    @GridToStringInclude
    private final Map<IgniteTxKey, IgniteTxEntry> txMapSnapshotPiece;

    /**
     * @param name Savepoint ID.
     * @param txMapSnapshotPiece State piece that should be saved in this savepoint.
     */
    TxSavepoint(String name, Map<IgniteTxKey, IgniteTxEntry> txMapSnapshotPiece) {
        assert name != null;
        assert txMapSnapshotPiece != null;

        this.name = name;

        this.txMapSnapshotPiece = txMapSnapshotPiece;
    }

    /**
     * @return Savepoint ID.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Entries stored in savepoint.
     */
    public Map<IgniteTxKey, IgniteTxEntry> getTxMapSnapshotPiece() {
        return txMapSnapshotPiece;
    }

    /**
     * @param key Key object.
     * @return True if savepoint contains entry with specified key.
     */
    public boolean containsKey(IgniteTxKey key) {
        return txMapSnapshotPiece.containsKey(key);
    }

    /**
     * Equality of savepoints depends on their IDs.
     *
     * @param o Another savepoint.
     * @return True if savepoints have equal names.
     */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TxSavepoint savepoint = (TxSavepoint)o;

        return name.equals(savepoint.name);
    }

    /** Hash code depends on savepoint ID. */
    @Override public int hashCode() {
        return name.hashCode();
    }
}
