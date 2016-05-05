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

import java.nio.ByteBuffer;

/**
 * Log entry abstract class.
 */
public abstract class WALRecord {
    /**
     * Record type.
     */
    public enum RecordType {
        /** */
        TX_RECORD,

        /** */
        PAGE_RECORD,

        /** */
        DATA_RECORD,

        /** */
        STORE_OPERATION_RECORD,

        /** */
        CHECKPOINT_RECORD;

        /** */
        private static final RecordType[] VALS = RecordType.values();

        /** */
        public static RecordType fromOrdinal(int ord) {
            return ord < 0 || ord >= VALS.length ? null : VALS[ord];
        }
    }

    /**
     * @return Entry type.
     */
    public abstract RecordType type();
}
