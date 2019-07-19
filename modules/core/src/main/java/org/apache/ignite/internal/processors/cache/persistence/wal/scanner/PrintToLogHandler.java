/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.scanner;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandler.toStringRecord;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.DEFAULT_WAL_RECORD_PREFIX;

/**
 * Handler which print record to log.
 *
 * This is not thread safe. Can be used only one time.
 */
class PrintToLogHandler implements ScannerHandler {
    /** */
    private final IgniteLogger log;

    /** */
    private StringBuilder resultString = new StringBuilder();

    /**
     * @param log Logger.
     */
    public PrintToLogHandler(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void handle(IgniteBiTuple<WALPointer, WALRecord> record) {
        ensureNotFinished();

        resultString
            .append(DEFAULT_WAL_RECORD_PREFIX)
            .append(toStringRecord(record.get2()))
            .append(System.lineSeparator());
    }

    /** {@inheritDoc} */
    @Override public void finish() {
        ensureNotFinished();

        String msg = resultString.toString();

        resultString = null;

        if (log.isInfoEnabled())
            log.info(msg);
    }

    /**
     *
     */
    private void ensureNotFinished() {
        if (resultString == null)
            throw new IgniteException("This handler has been already finished.");
    }
}
