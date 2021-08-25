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

package org.apache.ignite.distributed;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.raft.server.impl.JRaftServerImpl;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.storage.rocksdb.RocksDbStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.raft.client.service.ITAbstractListenerSnapshotTest;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;

/**
 * Persistent (rocksdb-based) partitions raft group snapshots tests.
 */
public class ITTablePersistenceTest extends ITAbstractListenerSnapshotTest<PartitionListener> {
    /** */
    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(UUID.randomUUID(),
        1,
        new Column[] {new Column("key", NativeTypes.INT64, false)},
        new Column[] {new Column("value", NativeTypes.INT64, false)}
    );

    /** */
    private static final Row FIRST_KEY = createKeyRow(0);

    /** */
    private static final Row FIRST_VALUE = createKeyValueRow(0, 0);

    /** */
    private static final Row SECOND_KEY = createKeyRow(1);

    /** */
    private static final Row SECOND_VALUE = createKeyValueRow(1, 1);

    /** {@inheritDoc} */
    @Override public void beforeFollowerStop(RaftGroupService service) throws Exception {
        var table = new InternalTableImpl("table", UUID.randomUUID(), Map.of(0, service), 1);

        table.upsert(FIRST_VALUE, null).get();
    }

    /** {@inheritDoc} */
    @Override public void afterFollowerStop(RaftGroupService service) throws Exception {
        var table = new InternalTableImpl("table", UUID.randomUUID(), Map.of(0, service), 1);

        // Remove the first key
        table.delete(FIRST_KEY, null).get();

        // Put deleted data again
        table.upsert(FIRST_VALUE, null).get();
    }

    /** {@inheritDoc} */
    @Override public void afterSnapshot(RaftGroupService service) throws Exception {
        var table = new InternalTableImpl("table", UUID.randomUUID(), Map.of(0, service), 1);

        table.upsert(SECOND_VALUE, null).get();
    }

    /** {@inheritDoc} */
    @Override public BooleanSupplier snapshotCheckClosure(JRaftServerImpl restarted, boolean interactedAfterSnapshot) {
        RocksDbStorage storage = (RocksDbStorage) getListener(restarted, raftGroupId()).getStorage();

        Row key = interactedAfterSnapshot ? SECOND_KEY : FIRST_KEY;
        Row value = interactedAfterSnapshot ? SECOND_VALUE : FIRST_VALUE;

        ByteBuffer buffer = key.keySlice();
        byte[] keyBytes = new byte[buffer.capacity()];
        buffer.get(keyBytes);

        SimpleDataRow finalRow = new SimpleDataRow(keyBytes, null);
        SimpleDataRow finalValue = new SimpleDataRow(keyBytes, value.bytes());

        return () -> {
            DataRow read = storage.read(finalRow);
            return Objects.equals(finalValue, read);
        };
    }

    /** {@inheritDoc} */
    @Override public Path getListenerPersistencePath(PartitionListener listener) {
        return ((RocksDbStorage) listener.getStorage()).getDbPath();
    }

    /** {@inheritDoc} */
    @Override public RaftGroupListener createListener(Path workDir) {
        return new PartitionListener(
            new RocksDbStorage(workDir.resolve(UUID.randomUUID().toString()), ByteBuffer::compareTo)
        );
    }

    /** {@inheritDoc} */
    @Override public String raftGroupId() {
        return "partitions";
    }

    /**
     * Creates a {@link Row} with the supplied key.
     *
     * @param id Key.
     * @return Row.
     */
    private static Row createKeyRow(long id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Creates a {@link Row} with the supplied key and value.
     *
     * @param id Key.
     * @param value Value.
     * @return Row.
     */
    private static Row createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }
}
