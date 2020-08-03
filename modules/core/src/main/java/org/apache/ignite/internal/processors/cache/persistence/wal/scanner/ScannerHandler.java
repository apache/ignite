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

package org.apache.ignite.internal.processors.cache.persistence.wal.scanner;

import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Scanner handler which provide ability to do some handling on each record during iteration.
 */
public interface ScannerHandler {
    /**
     * Handling one more record during iteration over WAL.
     *
     * @param record One more record from WAL.
     */
    void handle(IgniteBiTuple<WALPointer, WALRecord> record);

    /**
     * Method which called after all iteration would be finished.
     */
    default void finish() {
    }

    /**
     * Execute 'then' handler after 'this'.
     *
     * @param then Next handler for execution.
     * @return Composite handler.
     */
    default ScannerHandler andThen(ScannerHandler then) {
        ScannerHandler thiz = this;

        return new ScannerHandler() {
            @Override public void handle(IgniteBiTuple<WALPointer, WALRecord> record) {
                try {
                    thiz.handle(record);
                }
                finally {
                    then.handle(record);
                }
            }

            @Override public void finish() {
                try {
                    thiz.finish();
                }
                finally {
                    then.finish();
                }
            }
        };
    }

    /**
     * Make string from given wal record.
     *
     * @param walRecord Source WAL record.
     * @return Representation of WAL record.
     */
    public static String toStringRecord(WALRecord walRecord) {
        String walRecordStr;

        try {
            walRecordStr = walRecord != null ? walRecord.toString() : "Record is null";
        }
        catch (RuntimeException e) {
            walRecordStr = "Record : " + walRecord.type() + " - Unable to convert to string representation.";
        }
        return walRecordStr;
    }
}
