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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.Nullable;

import static java.nio.ByteBuffer.allocate;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.CRC_SIZE;

/**
 * Utility class for WAL testing.
 */
public class WalTestUtils {
    /**
     * Puts zero CRC to the one of a randomly choosen record for the specified segment if {@code random} is not null,
     * otherwsise to the last element.
     *
     * @param desc WAL segment descriptor.
     * @param iterFactory Iterator factory for segment iterating.
     * @param random Random generator, If it is null, returns a last element position.
     * @return Descriptor that is located strictly before the corrupted one.
     * @throws IOException If IO exception.
     * @throws IgniteCheckedException If iterator failed.
     */
    public static FileWALPointer corruptRandomWalRecord(
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

        corruptWalRecord(desc, pointer);

        return pointers.get(idxCorrupted - 1);
    }

    /**
     * Puts zero CRC to record that associated with {@code pointer} for the specified segment.
     *
     * @param desc WAL segment descriptor.
     * @param pointer WAL pointer.
     */
    public static void corruptWalRecord(
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
     * Put zero CRC in one of records for the specified compressed segment.
     *
     * @param desc WAL segment descriptor.
     * @param pointer WAL pointer.
     */
    public static void corruptWalRecordInCompressedSegment(
        FileDescriptor desc,
        FileWALPointer pointer
    ) throws IOException, IgniteCheckedException {
        File tmp = Files.createTempDirectory("temp-dir").toFile();

        U.unzip(desc.file(), tmp, new NullLogger());

        File walFile = tmp.listFiles()[0];

        String walFileName = desc.file().getName().replace(FilePageStoreManager.ZIP_SUFFIX, "");

        // reanaming is needed because unzip removes leading zeros from archived wal segment file name,
        // but strict name pattern of wal file is needed for WALIterator
        walFile.renameTo(new File(tmp.getPath() + "/" + walFileName));

        walFile = tmp.listFiles()[0];

        final IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(walFile);

        try (WALIterator stIt = factory.iterator(builder)) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();

                final WALRecord record = next.get2();

                if (pointer.equals(record.position())) {
                    corruptWalRecord(new FileDescriptor(walFile), (FileWALPointer)next.get1());
                    break;
                }
            }
        }

        byte[] fileBytes = U.zip(Files.readAllBytes(walFile.toPath()));

        Files.write(desc.file().toPath(), fileBytes);

        walFile.delete();

        tmp.delete();
    }

    /**
     * Corrupts zip file. According to section 4.3.7 of zip file specification, this method just sets 0
     * to the first 30 bytes of the first local file header to achieve corruption.
     *
     * @param desc File descriptor.
     * @see <a href="https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT">Zip file specification </a>
     */
    public static void corruptCompressedFile(FileDescriptor desc) throws IOException {
        int firstLocFileHdrOff = 0;

        ByteBuffer firstLocFileHdr = allocate(30);

        FileIOFactory ioFactory = new RandomAccessFileIOFactory();

        try (FileIO io = ioFactory.create(desc.file(), WRITE)) {
            io.write(firstLocFileHdr, firstLocFileHdrOff);

            io.force(true);
        }
    }
}
