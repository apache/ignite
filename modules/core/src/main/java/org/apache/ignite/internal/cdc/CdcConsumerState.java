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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.T2;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer.POINTER_SIZE;

/**
 * Change Data Capture Consumer state.
 *
 * Each time {@link CdcConsumer#onEvents(Iterator)} returns {@code true}
 * current offset in WAL segment and {@link DataEntry} index inside {@link DataRecord} saved to file.
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

    /** */
    public static final String MAPPINGS_STATE_FILE_NAME = "cdc-mappings-state" + FILE_SUFFIX;

    /** Long size in bytes. */
    private static final int LONG_SZ = 8;

    /** Integer size in bytes. */
    private static final int INT_SZ = 4;

    /** Log. */
    private final IgniteLogger log;

    /** WAL pointer state file. */
    private final Path walPtr;

    /** Temp WAL pointer state file. */
    private final Path tmpWalPtr;

    /** Types state file. */
    private final Path types;

    /** Temp types state file. */
    private final Path tmpTypes;

    /** Mappings state file. */
    private final Path mappings;

    /** Mappings types state file. */
    private final Path tmpMappings;

    /**
     * @param stateDir State directory.
     */
    public CdcConsumerState(IgniteLogger log, Path stateDir) {
        this.log = log.getLogger(CdcConsumerState.class);
        walPtr = stateDir.resolve(WAL_STATE_FILE_NAME);
        tmpWalPtr = stateDir.resolve(WAL_STATE_FILE_NAME + TMP_SUFFIX);
        types = stateDir.resolve(TYPES_STATE_FILE_NAME);
        tmpTypes = stateDir.resolve(TYPES_STATE_FILE_NAME + TMP_SUFFIX);
        mappings = stateDir.resolve(MAPPINGS_STATE_FILE_NAME);
        tmpMappings = stateDir.resolve(MAPPINGS_STATE_FILE_NAME + TMP_SUFFIX);
    }

    /**
     * Saves state to file.
     *
     * @param state WAL pointer and index of {@link DataEntry} inside {@link DataRecord}.
     */
    public void save(T2<WALPointer, Integer> state) throws IOException {
        save(() -> {
            ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

            buf.putLong(state.get1().index());
            buf.putInt(state.get1().fileOffset());
            buf.putInt(state.get2());
            buf.flip();

            return buf;
        }, tmpWalPtr, walPtr);
    }

    /**
     * Saves types state to file.
     * @param typesState State of types.
     */
    public void saveTypes(Map<Integer, Long> typesState) throws IOException {
        save0(typesState, tmpTypes, types);
    }

    /**
     * Saves mappings state to file
     * @param mappingsState Mappings state.
     */
    public void save(Set<T2<Integer, Byte>> mappingsState) throws IOException {
        save(() -> {
            ByteBuffer buf = ByteBuffer.allocate(INT_SZ + (INT_SZ + 1 /* byte */) * mappingsState.size());

            buf.putInt(mappingsState.size());

            for (T2<Integer, Byte> entry : mappingsState) {
                buf.putInt(entry.get1());
                buf.put(entry.get2());
            }

            buf.flip();

            return buf;
        }, tmpMappings, mappings);
    }

    /**
     * Loads CDC mappings state from file.
     *
     * @return Saved state.
     */
    public Set<T2<Integer, Byte>> loadMappings() {
        Set<T2<Integer, Byte>> state = load(mappings, ch -> {
            ByteBuffer buf = ByteBuffer.allocate(INT_SZ);

            int read = ch.read(buf);

            if (read != INT_SZ)
                return null;

            buf.flip();

            int sz = buf.getInt();
            int dataAmount = sz * (INT_SZ + 1 /* byte */);

            buf = ByteBuffer.allocate(dataAmount);

            read = ch.read(buf);

            if (read != dataAmount)
                return null;

            buf.flip();

            Set<T2<Integer, Byte>> data = new HashSet<>();

            for (int i = 0; i < sz; i++) {
                int typeId = buf.getInt();
                byte platform = buf.get();

                data.add(new T2<>(typeId, platform));
            }

            return data;
        });

        log.info("Initial mappings state loaded [mappingsCnt=" + (state != null ? state.size() : 0) + ']');

        if (state != null && log.isDebugEnabled()) {
            for (T2<Integer, Byte> m : state)
                log.debug("Mapping [typeId=" + m.get1() + ", platform=" + m.get2() + ']');
        }

        return state != null ? state : new HashSet<>();
    }

    /**
     * Loads CDC types state from file.
     *
     * @return Saved state.
     */
    public Map<Integer, Long> loadTypes() {
        Map<Integer, Long> state = load0(types);

        log.info("Initial types state loaded [typesCnt=" + state.size() + ']');

        if (log.isDebugEnabled()) {
            for (Map.Entry<Integer, Long> e : state.entrySet())
                log.debug("Type [typeId=" + e.getKey() + ", lastModified=" + e.getValue() + ']');
        }

        return state;
    }

    /**
     * Loads CDC consumer state from file.
     *
     * @return Saved state.
     */
    public T2<WALPointer, Integer> loadWal() {
        T2<WALPointer, Integer> state = load(walPtr, ch -> {
            ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

            int read = ch.read(buf);

            if (read != POINTER_SIZE)
                return null;

            buf.flip();

            long idx = buf.getLong();
            int offset = buf.getInt();
            int entryIdx = buf.getInt();

            return new T2<>(new WALPointer(idx, offset, 0), entryIdx);
        });

        if (state != null && log.isInfoEnabled())
            log.info("Initial WAL state loaded [ptr=" + state.get1() + ", idx=" + state.get2() + ']');

        return state;
    }

    /** Saves state to file. */
    private void save0(Map<Integer, Long> state, Path tmpFile, Path file) throws IOException {
        save(() -> {
            ByteBuffer buf = ByteBuffer.allocate(INT_SZ + (LONG_SZ + INT_SZ) * state.size());

            buf.putInt(state.size());

            for (Map.Entry<Integer, Long> entry : state.entrySet()) {
                buf.putInt(entry.getKey());
                buf.putLong(entry.getValue());
            }

            buf.flip();

            return buf;
        }, tmpFile, file);
    }

    /** Loads state from file. */
    private Map<Integer, Long> load0(Path file) {
        Map<Integer, Long> state = load(file, ch -> {
            ByteBuffer buf = ByteBuffer.allocate(INT_SZ);

            int read = ch.read(buf);

            if (read != INT_SZ)
                return null;

            buf.flip();

            int sz = buf.getInt();

            int dataAmount = sz * (LONG_SZ + INT_SZ);

            buf = ByteBuffer.allocate(dataAmount);

            read = ch.read(buf);

            if (read != dataAmount)
                return null;

            buf.flip();

            Map<Integer, Long> data = new HashMap<>();

            for (int i = 0; i < sz; i++) {
                int typeId = buf.getInt();
                long timestamp = buf.getLong();

                data.put(typeId, timestamp);
            }

            return data;
        });

        return state != null ? state : new HashMap<>();
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
