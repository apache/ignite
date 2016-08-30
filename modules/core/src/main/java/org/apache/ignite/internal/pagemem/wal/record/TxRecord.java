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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class TxRecord extends WALRecord {
    /**
     * Tx action enum.
     */
    public enum TxAction {
        /** Transaction begin. */
        BEGIN,

        /** Transaction prepare. */
        PREPARE,

        /** Transaction commit. */
        COMMIT,

        /** Transaction rollback. */
        ROLLBACK;

        /** Available values. */
        private static final TxAction[] VALS = TxAction.values();

        /**
         * Gets tx action value from ordinal.
         *
         * @param ord Ordinal.
         * @return Value.
         */
        public static TxAction fromOrdinal(int ord) {
            return ord < 0 || ord >= VALS.length ? null : VALS[ord];
        }
    }

    /** */
    private TxAction action;

    /** */
    private GridCacheVersion nearXidVer;

    /** */
    private GridCacheVersion dhtVer;

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.TX_RECORD;
    }

    /**
     * @return Near xid version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @param nearXidVer Near xid version.
     */
    public void nearXidVersion(GridCacheVersion nearXidVer) {
        this.nearXidVer = nearXidVer;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @param dhtVer DHT version.
     */
    public void dhtVersion(GridCacheVersion dhtVer) {
        this.dhtVer = dhtVer;
    }

    /**
     * @return Action.
     */
    public TxAction action() {
        return action;
    }

    /**
     * @param action Action.
     */
    public void action(TxAction action) {
        this.action = action;
    }
}
