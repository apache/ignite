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
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
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

    /** */
    public static final String CACHES_STATE_FILE_NAME = "cdc-caches-state" + FILE_SUFFIX;

    /**
     * The file stores state of CDC mode. Content of the file is a {@link CdcMode} value:
     * <ul>
     *     <li>{@link CdcMode#CDC_UTILITY_ACTIVE} means that {@link CdcMain} utility captures data.</li>
     *     <li>{@link CdcMode#IGNITE_NODE_ACTIVE} means that {@link CdcManager} captures data within Ignite node.</li>
     * </ul>
     */
    public static final String CDC_MODE_FILE_NAME = "cdc-mode" + FILE_SUFFIX;

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

    /** Cache state file. */
    private final Path caches;

    /** Mappings types state file. */
    private final Path tmpCaches;

    /** CDC manager mode state file. */
    private final Path cdcMode;

    /** Temp CDC manager mode state file. */
    private final Path tmpCdcMode;

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
        caches = stateDir.resolve(CACHES_STATE_FILE_NAME);
        tmpCaches = stateDir.resolve(CACHES_STATE_FILE_NAME + TMP_SUFFIX);
        cdcMode = stateDir.resolve(CDC_MODE_FILE_NAME);
        tmpCdcMode = stateDir.resolve(CDC_MODE_FILE_NAME + TMP_SUFFIX);
    }

    /**
     * Saves WAL consumption state to file.
     *
     * @param state WAL pointer and index of {@link DataEntry} inside {@link DataRecord}.
     */
    public void saveWal(T2<WALPointer, Integer> state) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

        buf.putLong(state.get1().index());
        buf.putInt(state.get1().fileOffset());
        buf.putInt(state.get2());
        buf.flip();

        try (FileChannel ch = FileChannel.open(tmpWalPtr, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ch.write(buf);

            ch.force(true);
        }

        Files.move(tmpWalPtr, walPtr, ATOMIC_MOVE, REPLACE_EXISTING);
    }

    /**
     * Saves binary types state to file.
     *
     * @param typesState State of types.
     */
    public void saveTypes(Map<Integer, Long> typesState) throws IOException {
        save(typesState, tmpTypes, types);
    }

    /**
     * Saves types mappings state to file.
     *
     * @param mappingsState Mappings state.
     */
    public void saveMappings(Set<T2<Integer, Byte>> mappingsState) throws IOException {
        save(mappingsState, tmpMappings, mappings);
    }

    /**
     * Loads CDC consumer state from file.
     *
     * @return Saved state.
     */
    public T2<WALPointer, Integer> loadWalState() {
        if (!Files.exists(walPtr))
            return null;

        T2<WALPointer, Integer> state;

        try (FileChannel ch = FileChannel.open(walPtr, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

            int read = ch.read(buf);

            if (read != POINTER_SIZE)
                return null;

            buf.flip();

            long idx = buf.getLong();
            int offset = buf.getInt();
            int entryIdx = buf.getInt();

            state = new T2<>(new WALPointer(idx, offset, 0), entryIdx);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to read state [file=" + walPtr + ']', e);
        }

        if (log.isInfoEnabled())
            log.info("Initial WAL state loaded [ptr=" + state.get1() + ", idx=" + state.get2() + ']');

        return state;
    }

    /**
     * Loads CDC caches state from file.
     *
     * @return Saved state.
     */
    public Map<Integer, Long> loadCaches() {
        Map<Integer, Long> state = load(caches, HashMap::new);

        log.info("Initial caches state loaded [cachesCnt=" + state.size() + ']');

        if (log.isDebugEnabled()) {
            for (Map.Entry<Integer, Long> entry : state.entrySet())
                log.debug("Cache [cacheId=" + entry.getKey() + ", lastModified=" + entry.getValue() + ']');
        }

        return state;
    }

    /**
     * Saves caches state to file.
     * @param cachesState State of caches.
     */
    public void saveCaches(Map<Integer, Long> cachesState) throws IOException {
        save(cachesState, tmpCaches, caches);
    }

    /**
     * Loads types mappings state from file.
     *
     * @return Saved state.
     */
    public Set<T2<Integer, Byte>> loadMappingsState() {
        Set<T2<Integer, Byte>> state = load(mappings, HashSet::new);

        assert state != null;

        log.info("Initial mappings state loaded [mappingsCnt=" + state.size() + ']');

        if (log.isDebugEnabled()) {
            for (T2<Integer, Byte> m : state)
                log.debug("Mapping [typeId=" + m.get1() + ", platform=" + m.get2() + ']');
        }

        return state;
    }

    /**
     * Loads CDC types state from file.
     *
     * @return Saved state.
     */
    public Map<Integer, Long> loadTypesState() {
        Map<Integer, Long> state = load(types, HashMap::new);

        assert state != null;

        log.info("Initial types state loaded [typesCnt=" + state.size() + ']');

        if (log.isDebugEnabled()) {
            for (Map.Entry<Integer, Long> e : state.entrySet())
                log.debug("Type [typeId=" + e.getKey() + ", lastModified=" + e.getValue() + ']');
        }

        return state;
    }

    /** Save object to file. */
    private <T> void save(T state, Path tmp, Path file) throws IOException {
        try (ObjectOutput oos = new ObjectOutputStream(Files.newOutputStream(tmp))) {
            oos.writeObject(state);
        }

        Files.move(tmp, file, ATOMIC_MOVE, REPLACE_EXISTING);
    }

    /** Loads data from path. */
    private <D> D load(Path state, Supplier<D> dflt) {
        if (!Files.exists(state))
            return dflt.get();

        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(state))) {

            return (D)ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Loads CDC mode state from file.
     *
     * @return CDC mode state.
     */
    public CdcMode loadCdcMode() {
        CdcMode state = load(cdcMode, () -> CdcMode.IGNITE_NODE_ACTIVE);

        log.info("CDC mode loaded [" + state + ']');

        return state;
    }

    /**
     * Saves CDC mode state to file.
     *
     * @param mode CDC mode.
     */
    public void saveCdcMode(CdcMode mode) throws IOException {
        save(mode, tmpCdcMode, cdcMode);
    }
}
