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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.crc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static java.nio.ByteBuffer.allocate;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.CRC_SIZE;

/**
 * Utility class for WAL testing.
 */
public class WalTestUtils {
    /**
     * Put zero CRC in one of records for the specified segment.
     *
     * @param desc WAL segment descriptor.
     * @param iterFactory Iterator factory for segment iterating.
     * @param random Random generator, If it is null, returns a last element position.
     * @return Descriptor that is located strictly before the corrupted one.
     * @throws IOException If IO exception.
     * @throws IgniteCheckedException If iterator failed.
     */
    public static FileWALPointer corruptWalSegmentFile(
        FileDescriptor desc,
        IgniteWalIteratorFactory iterFactory,
        @Nullable Random random
    ) throws IOException, IgniteCheckedException {
        List<FileWALPointer> pointers = new ArrayList<>();

        try (WALIterator it = iterFactory.iterator(desc.file())) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it)
                pointers.add((FileWALPointer)tuple.get1());
        }

        // Should have a previous record to return and another value before that to ensure that "lastReadPtr"
        // in a test will always exist.
        int idxCorrupted = random != null ? 2 + random.nextInt(pointers.size() - 2) : pointers.size() - 1;

        FileWALPointer pointer = pointers.get(idxCorrupted);

        corruptWalSegmentFile(desc, pointer);

        return pointers.get(idxCorrupted - 1);
    }

    /**
     * Put zero CRC in one of records for the specified segment.
     *
     * @param desc WAL segment descriptor.
     * @param pointer WAL pointer.
     */
    public static void corruptWalSegmentFile(
        FileDescriptor desc,
        FileWALPointer pointer
    ) throws IOException {

        int crc32Off = pointer.fileOffset() + pointer.length() - CRC_SIZE;

        ByteBuffer zeroCrc32 = allocate(CRC_SIZE); // Has 0 value by default.

        FileIOFactory ioFactory = new RandomAccessFileIOFactory();
        try (FileIO io = ioFactory.create(desc.file(), WRITE)) {
            io.write(zeroCrc32, crc32Off);

            io.force(true);
        }
    }

    /**
     * @param desc Wal segment.
     * @param iterFactory Iterator factory.
     * @param recordType filter by RecordType
     * @return List of pointers.
     */
    public static List<FileWALPointer> getPointers(
        FileDescriptor desc,
        IgniteWalIteratorFactory iterFactory,
        WALRecord.RecordType recordType
    ) throws IgniteCheckedException {
        List<FileWALPointer> cpPointers = new ArrayList<>();

        try (WALIterator it = iterFactory.iterator(desc.file())) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                if (recordType.equals(tuple.get2().type()))
                    cpPointers.add((FileWALPointer)tuple.get1());
            }
        }

        return cpPointers;
    }

    /**
     * @param desc Wal segment.
     * @param iterFactory Iterator factory.
     * @param recordPurpose Filter by RecordPurpose
     * @return List of pointers.
     */
    public static List<FileWALPointer> getPointers(
        FileDescriptor desc,
        IgniteWalIteratorFactory iterFactory,
        WALRecord.RecordPurpose recordPurpose
    ) throws IgniteCheckedException {
        List<FileWALPointer> cpPointers = new ArrayList<>();

        try (WALIterator it = iterFactory.iterator(desc.file())) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                if (recordPurpose.equals(tuple.get2().type().purpose()))
                    cpPointers.add((FileWALPointer)tuple.get1());
            }
        }

        return cpPointers;
    }
}
