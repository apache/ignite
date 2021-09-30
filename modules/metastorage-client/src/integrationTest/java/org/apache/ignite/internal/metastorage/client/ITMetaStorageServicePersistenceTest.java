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

package org.apache.ignite.internal.metastorage.client;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.persistence.RocksDBKeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetaStorageListener;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.raft.client.service.ITAbstractListenerSnapshotTest;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Persistent (rocksdb-based) meta storage raft group snapshots tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ITMetaStorageServicePersistenceTest extends ITAbstractListenerSnapshotTest<MetaStorageListener> {
    /** */
    private static final ByteArray FIRST_KEY = ByteArray.fromString("first");

    /** */
    private static final byte[] FIRST_VALUE = "firstValue".getBytes(StandardCharsets.UTF_8);

    /** */
    private static final ByteArray SECOND_KEY = ByteArray.fromString("second");

    /** */
    private static final byte[] SECOND_VALUE = "secondValue".getBytes(StandardCharsets.UTF_8);

    /** */
    private MetaStorageServiceImpl metaStorage;

    /** */
    private KeyValueStorage storage;

    /** */
    @AfterEach
    void tearDown() throws Exception {
        if (storage != null)
            storage.close();
    }

    /** {@inheritDoc} */
    @Override public void beforeFollowerStop(RaftGroupService service) throws Exception {
        metaStorage = new MetaStorageServiceImpl(service, null);

        // Put some data in the metastorage
        metaStorage.put(FIRST_KEY, FIRST_VALUE).get();

        // Check that data has been written successfully
        check(metaStorage, new EntryImpl(FIRST_KEY, FIRST_VALUE, 1, 1));;
    }

    /** {@inheritDoc} */
    @Override public void afterFollowerStop(RaftGroupService service) throws Exception {
        // Remove the first key from the metastorage
        metaStorage.remove(FIRST_KEY).get();

        // Check that data has been removed
        check(metaStorage, new EntryImpl(FIRST_KEY, null, 2, 2));

        // Put same data again
        metaStorage.put(FIRST_KEY, FIRST_VALUE).get();

        // Check that it has been written
        check(metaStorage, new EntryImpl(FIRST_KEY, FIRST_VALUE, 3, 3));
    }

    /** {@inheritDoc} */
    @Override public void afterSnapshot(RaftGroupService service) throws Exception {
        metaStorage.put(SECOND_KEY, SECOND_VALUE).get();
    }

    /** {@inheritDoc} */
    @Override public BooleanSupplier snapshotCheckClosure(JRaftServerImpl restarted, boolean interactedAfterSnapshot) {
        KeyValueStorage storage = getListener(restarted, raftGroupId()).getStorage();

        byte[] lastKey = interactedAfterSnapshot ? SECOND_KEY.bytes() : FIRST_KEY.bytes();
        byte[] lastValue = interactedAfterSnapshot ? SECOND_VALUE : FIRST_VALUE;

        int expectedRevision = interactedAfterSnapshot ? 4 : 3;
        int expectedUpdateCounter = interactedAfterSnapshot ? 4 : 3;

        EntryImpl expectedLastEntry = new EntryImpl(new ByteArray(lastKey), lastValue, expectedRevision, expectedUpdateCounter);

        return () -> {
            org.apache.ignite.internal.metastorage.server.Entry e = storage.get(lastKey);
            return e.empty() == expectedLastEntry.empty()
                && e.tombstone() == expectedLastEntry.tombstone()
                && e.revision() == expectedLastEntry.revision()
                && e.updateCounter() == expectedLastEntry.revision()
                && Arrays.equals(e.key(), expectedLastEntry.key().bytes())
                && Arrays.equals(e.value(), expectedLastEntry.value());
        };
    }

    /** {@inheritDoc} */
    @Override public Path getListenerPersistencePath(MetaStorageListener listener) {
        return ((RocksDBKeyValueStorage) listener.getStorage()).getDbPath();
    }

    /** {@inheritDoc} */
    @Override public RaftGroupListener createListener(Path listenerPersistencePath) {
        storage = new RocksDBKeyValueStorage(listenerPersistencePath);

        storage.start();

        return new MetaStorageListener(storage);
    }

    /** {@inheritDoc} */
    @Override public String raftGroupId() {
        return "metastorage";
    }

    /**
     * Check meta storage entry.
     *
     * @param metaStorage Meta storage service.
     * @param expected Expected entry.
     * @throws ExecutionException If failed.
     * @throws InterruptedException If failed.
     */
    private static void check(MetaStorageServiceImpl metaStorage, EntryImpl expected)
        throws ExecutionException, InterruptedException {
        Entry entry = metaStorage.get(expected.key()).get();

        assertEquals(expected, entry);
    }
}
