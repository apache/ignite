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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.IgniteCheckedException;

/**
 * This exception is thrown either when we reach the end of file of WAL segment, or when we encounter
 * a record with type equal to
 * {@link org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType#STOP_ITERATION_RECORD_TYPE}
 */
public class SegmentEofException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public final boolean switchSegmentRecord;

    /**
     * @param msg Message.
     * @param cause Cause.
     */
    public SegmentEofException(String msg, Throwable cause) {
        this(msg, cause, false);
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     * @param switchSegmentRecord Switch segmment record flag.
     */
    public SegmentEofException(String msg, Throwable cause, boolean switchSegmentRecord) {
        super(msg, cause, false);

        this.switchSegmentRecord = switchSegmentRecord;
    }

    /** @return Switch segment record value. */
    public boolean isSwitchSegmentRecord() {
        return switchSegmentRecord;
    }
}
