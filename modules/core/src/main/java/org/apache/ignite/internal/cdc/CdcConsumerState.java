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
import java.util.Iterator;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;

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
    public static final String STATE_FILE_NAME = "cdc-state" + FILE_SUFFIX;

    /** State file. */
    private final Path state;

    /** Temp state file. */
    private final Path tmp;

    /**
     * @param stateDir State directory.
     */
    public CdcConsumerState(Path stateDir) {
        state = stateDir.resolve(STATE_FILE_NAME);
        tmp = stateDir.resolve(STATE_FILE_NAME + TMP_SUFFIX);
    }

    /**
     * Saves state to file.
     * @param ptr WAL pointer.
     */
    public void save(WALPointer ptr) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

        buf.putLong(ptr.index());
        buf.putInt(ptr.fileOffset());
        buf.putInt(ptr.length());
        buf.flip();

        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ch.write(buf);

            ch.force(true);
        }

        Files.move(tmp, state, ATOMIC_MOVE, REPLACE_EXISTING);
    }

    /**
     * Loads CDC state from file.
     * @return Saved state.
     */
    public WALPointer load() {
        if (!Files.exists(state))
            return null;

        try (FileChannel ch = FileChannel.open(state, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(POINTER_SIZE);

            ch.read(buf);

            buf.flip();

            long idx = buf.getLong();
            int offset = buf.getInt();
            int length = buf.getInt();

            return new WALPointer(idx, offset, length);
        }
        catch (IOException e) {
            throw new IgniteException("Failed to read state [file=" + state + ']', e);
        }

    }
}
