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

package org.apache.ignite.internal.cdc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer.POINTER_SIZE;

/**
 * Change Data Capture Consumer state.
 *
 * Each time {@link CdcConsumer#onEvents(Iterator)} returns {@code true}
 * current offset in WAL segment saved to file.
 * This allows to the {@link CdcConsumer} to continue consumption of the {@link CdcEvent}
 * from the last saved offset in case of fail or restart.
 *
 * @see CdcConsumer#onEvents(Iterator)
 * @see CdcMain
 */
public class CdcConsumerState {
    /** */
    public static final String WAL_STATE_FILE_NAME = "cdc-wal-state" + FILE_SUFFIX;

    /** */
    public static final String TYPES_STATE_FILE_NAME = "cdc-types-state" + FILE_SUFFIX;

    /** Long size in bytes. */
    private static final int LONG_SZ = 8;

    /** Integer size in bytes. */
    private static final int INT_SZ = 4;


    /** WAL pointer state file. */
    private final Path walPtr;

    /** Temp WAL pointer state file. */
    private final Path tmpWalPtr;

    /** Types state file. */
    private final Path types;

    /** Temp types state file. */
    private final Path tmpTypes;

    /**
     * @param stateDir State directory.
     */
    public CdcConsumerState(Path stateDir) {
        walPtr = stateDir.resolve(WAL_STATE_FILE_NAME);
        tmpWalPtr = stateDir.resolve(WAL_STATE_FILE_NAME + TMP_SUFFIX);
        types = stateDir.resolve(TYPES_STATE_FILE_NAME);
        tmpTypes = stateDir.resolve(TYPES_STATE_FILE_NAME + TMP_SUFFIX);
    }

    /**
     * Saves WAL pointer state to file.
     * @param ptr WAL pointer.
     */
    public void save(WALPointer ptr) throws IOException {
        save(() -> {
            ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

            buf.putLong(ptr.index());
            buf.putInt(ptr.fileOffset());
            buf.putInt(ptr.length());
            buf.flip();

            return buf;
        }, tmpWalPtr, walPtr);
    }

    /**
     * Saves types state to file.
     * @param typesState State of types.
     */
    public void save(Map<Integer, Long> typesState) throws IOException {
        save(() -> {
            ByteBuffer buf = ByteBuffer.allocate(INT_SZ + (LONG_SZ + INT_SZ) * typesState.size());

            buf.putInt(typesState.size());

            for (Map.Entry<Integer, Long> entry : typesState.entrySet()) {
                buf.putInt(entry.getKey());
                buf.putLong(entry.getValue());
            }

            buf.flip();

            return buf;
        }, tmpTypes, types);
    }

    /**
     * Loads CDC state from file.
     * @return Saved state.
     */
    public IgniteBiTuple<WALPointer, Map<Integer, Long>> load() {
        WALPointer walState = load(walPtr, ch -> {
            ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

            int read = ch.read(buf);

            if (read != POINTER_SIZE)
                return null;

            buf.flip();

            long idx = buf.getLong();
            int offset = buf.getInt();
            int length = buf.getInt();

            return new WALPointer(idx, offset, length);
        });

        Map<Integer, Long> typesState = load(types, ch -> {
            ByteBuffer buf = ByteBuffer.allocate(LONG_SZ);

            int read = ch.read(buf);

            if (read != LONG_SZ)
                return null;

            buf.flip();

            int sz = buf.getInt();;

            buf = ByteBuffer.allocate(sz * (LONG_SZ + INT_SZ));

            read = ch.read(buf);

            if (read != sz * (LONG_SZ + INT_SZ))
                return null;

            Map<Integer, Long> data = new HashMap<>();

            for (int i = 0; i < sz; i++) {
                int typeId = buf.getInt();
                long timestamp = buf.getLong();

                data.put(typeId, timestamp);
            }

            return data;
        });

        return F.t(walState, typesState);
    }

    /**
     * @param file File to load from.
     * @param ldr Object loader.
     * @param <T> Result type.
     * @return Result loaded from file.
     */
    private static <T> T load(Path file, IOFunction<FileChannel, T> ldr) {
        if (!Files.exists(file))
            return null;

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            return ldr.apply(ch);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to read state [file=" + file + ']', e);
        }
    }

    /**
     * @param bytes Bytes to save supplier.
     * @param tmp Temp file.
     * @param file Destination file.
     * @throws IOException In case of error.
     */
    private static void save(Supplier<ByteBuffer> bytes, Path tmp, Path file) throws IOException {
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ch.write(bytes.get());
            ch.force(true);
        }

        Files.move(tmp, file, ATOMIC_MOVE, REPLACE_EXISTING);
    }

    /** */
    @FunctionalInterface
    private interface IOFunction<T, R> {
        /** */
        R apply(T var) throws IOException;
    }
}
