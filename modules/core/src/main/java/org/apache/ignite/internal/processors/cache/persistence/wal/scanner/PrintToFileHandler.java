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
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.DEFAULT_WAL_RECORD_PREFIX;

/**
 * Handler which print record to file.
 */
class PrintToFileHandler implements ScannerHandler {
    /** */
    private final FileIOFactory ioFactory;

    /** Target file. */
    private final File file;

    /** Open file to write. */
    private FileIO fileToWrite;

    /**
     * @param file File to write.
     * @param ioFactory File IO factory.
     */
    public PrintToFileHandler(File file, FileIOFactory ioFactory) {
        this.file = file;
        this.ioFactory = ioFactory != null ? ioFactory : new DataStorageConfiguration().getFileIOFactory();
    }

    /** {@inheritDoc} */
    @Override public void handle(IgniteBiTuple<WALPointer, WALRecord> record) {
        initIfRequired();

        byte[] writes = (DEFAULT_WAL_RECORD_PREFIX + record.get2() + "\n").getBytes(Charset.forName("utf-8"));

        int written = 0;

        try {
            while ((written += fileToWrite.writeFully(writes, written, writes.length - written)) < writes.length);
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
    }

    /**
     * Initialize fileToWrite if it required.
     */
    private void initIfRequired() {
        if (fileToWrite == null) {
            try {
                fileToWrite = ioFactory.create(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finish() {
        if (fileToWrite == null)
            return;

        try {
            fileToWrite.force();
            fileToWrite.close();
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
    }
}
