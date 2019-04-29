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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.DEFAULT_WAL_RECORD_PREFIX;

/**
 * Handler which print record to log.
 */
class PrintToLogHandler implements ScannerHandler {
    /** */
    private final IgniteLogger log;

    /** */
    private final StringBuilder resultString = new StringBuilder();

    /**
     * @param log Logger.
     */
    public PrintToLogHandler(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void handle(IgniteBiTuple<WALPointer, WALRecord> record) {
        resultString.append(DEFAULT_WAL_RECORD_PREFIX).append(record.get2()).append("\n");
    }

    /** {@inheritDoc} */
    @Override public void finish() {
        log.info(resultString.toString());
    }
}
