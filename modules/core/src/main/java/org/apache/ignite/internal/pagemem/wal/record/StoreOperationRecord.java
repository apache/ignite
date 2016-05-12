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

/**
 *
 */
public class StoreOperationRecord extends WALRecord {
    /**
     * Store operation type.
     */
    public enum StoreOperationType {
        /** */
        ENTRY_CREATE,

        /** */
        INDEX_PUT,

        /** */
        INDEX_REMOVE;

        /** */
        private static final StoreOperationType[] VALS = StoreOperationType.values();

        /** */
        public static StoreOperationType fromOrdinal(int ord) {
            return ord < 0 || ord >= VALS.length ? null : VALS[ord];
        }
    }

    /** */
    private StoreOperationType opType;

    /** */
    private int cacheId;

    /** */
    private long link;

    /** */
    private int idxId;

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.STORE_OPERATION_RECORD;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Link to data.
     */
    public long link() {
        return link;
    }

    /**
     * @return Index ID.
     */
    public int indexId() {
        return idxId;
    }

    /**
     * @return Operation type.
     */
    public StoreOperationType operationType() {
        return opType;
    }

    /**
     * @param opType Operation type.
     */
    public void operationType(StoreOperationType opType) {
        this.opType = opType;
    }

    /**
     * @param cacheId Cache ID.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /**
     * @param link Link.
     */
    public void link(long link) {
        this.link = link;
    }

    /**
     * @param idxId Index ID.
     */
    public void indexId(int idxId) {
        this.idxId = idxId;
    }
}
