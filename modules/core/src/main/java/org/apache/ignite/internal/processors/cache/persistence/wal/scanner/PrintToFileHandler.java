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
import java.nio.file.StandardOpenOption;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandler.toStringRecord;
import static org.apache.ignite.internal.processors.cache.persistence.wal.scanner.ScannerHandlers.DEFAULT_WAL_RECORD_PREFIX;

/**
 * Handler which print record to file.
 *
 * This is not thread safe. Can be used only one time.
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
    @Override public final void handle(IgniteBiTuple<WALPointer, WALRecord> record) {
        initIfRequired();

        writeFully(getBytes(record));
    }

    /**
     * @param record WAL record with its pointer.
     * @return Bytes repersentation of data to be written in dump file.
     */
    protected byte[] getBytes(IgniteBiTuple<WALPointer, WALRecord> record) {
        return (DEFAULT_WAL_RECORD_PREFIX + toStringRecord(record.get2()) + System.lineSeparator()).getBytes(UTF_8);
    }

    /**
     * @return Optional header for the diagnostic file. {@code null} if there should be no header.
     */
    @Nullable protected byte[] getHeader() {
        return null;
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

        byte[] hdr = getHeader();

        if (hdr != null)
            writeFully(hdr);
    }

    /**
     * Write byte array into file.
     *
     * @param bytes Data.
     * @throws IgniteException If write failed.
     */
    private void writeFully(byte[] bytes) {
        int written = 0;

        try {
            while ((written += fileToWrite.writeFully(bytes, written, bytes.length - written)) < bytes.length);
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
    }

    /** {@inheritDoc} */
    @Override public void finish() {
        if (fileToWrite == null)
            return;

        try {
            try {
                fileToWrite.force();
            }
            finally {
                fileToWrite.close();
            }
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
    }
}
