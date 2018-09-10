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

package org.apache.ignite.internal.processors.cache.persistence.wal.crc;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

/**
 * Will be thrown for WAL records with zero CRC where CRC check is the only reading error.
 */
public class IgniteWalRecordZeroCrcException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** WAL record */
    private final WALRecord walRecord;

    /**
     * @param cause Cause exception, presumably {@link IgniteDataIntegrityViolationException}
     * @param record Fully read WAL record that caused exception on closing
     */
    public IgniteWalRecordZeroCrcException(Throwable cause, WALRecord record) {
        super(cause);
        walRecord = record;
    }

    /**
     * @return WAL record that was completely read but failed on CRC check and written CRC was zero.
     */
    public WALRecord getWalRecord() {
        return walRecord;
    }
}
