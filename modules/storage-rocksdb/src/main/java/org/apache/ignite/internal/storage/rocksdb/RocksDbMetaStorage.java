/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.storage.StorageException;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

/**
 * Wrapper around the "meta" Column Family inside a RocksDB-based storage, which stores some auxiliary information needed for internal
 * storage logic.
 */
class RocksDbMetaStorage implements AutoCloseable {
    /**
     * Name of the key that corresponds to a list of existing partition IDs of a storage.
     */
    private static final byte[] PARTITION_ID_PREFIX = "part".getBytes(StandardCharsets.UTF_8);

    /**
     * Name of the key that is out of range of the partition ID key prefix, used as an exclusive bound.
     */
    private static final byte[] PARTITION_ID_PREFIX_END;

    static {
        PARTITION_ID_PREFIX_END = PARTITION_ID_PREFIX.clone();

        PARTITION_ID_PREFIX_END[PARTITION_ID_PREFIX_END.length - 1] += 1;
    }

    private final ColumnFamily metaCf;

    RocksDbMetaStorage(ColumnFamily metaCf) {
        this.metaCf = metaCf;
    }

    /**
     * Returns a list of partition IDs that exist in the associated storage.
     *
     * @return list of partition IDs
     */
    int[] getPartitionIds() {
        Stream.Builder<byte[]> data = Stream.builder();

        try (
                var upperBound = new Slice(PARTITION_ID_PREFIX_END);
                var options = new ReadOptions().setIterateUpperBound(upperBound);
                RocksIterator it = metaCf.newIterator(options);
        ) {
            it.seek(PARTITION_ID_PREFIX);

            RocksUtils.forEach(it, (key, value) -> data.add(value));
        } catch (RocksDBException e) {
            throw new StorageException("Error when reading a list of partition IDs from the meta Column Family", e);
        }

        return data.build()
                .mapToInt(RocksDbMetaStorage::bytesToUnsignedShort)
                .toArray();
    }

    /**
     * Converts a byte array (in big endian format) into an unsigned short (represented as an {@code int}).
     */
    private static int bytesToUnsignedShort(byte[] array) {
        assert array.length == 2 : array.length;

        return array[0] << 8 | array[1];
    }

    /**
     * Saves the given partition ID into the meta Column Family.
     *
     * @param partitionId partition ID
     */
    void putPartitionId(int partitionId) {
        byte[] partitionIdKey = partitionIdKey(partitionId);

        byte[] partitionIdBytes = Arrays.copyOfRange(partitionIdKey, PARTITION_ID_PREFIX.length, partitionIdKey.length);

        try {
            metaCf.put(partitionIdKey, partitionIdBytes);
        } catch (RocksDBException e) {
            throw new StorageException("Unable to save partition " + partitionId + " in the meta Column Family", e);
        }
    }

    /**
     * Removes the given partition ID from the meta Column Family.
     *
     * @param partitionId partition ID
     */
    void removePartitionId(int partitionId) {
        try {
            metaCf.delete(partitionIdKey(partitionId));
        } catch (RocksDBException e) {
            throw new StorageException("Unable to delete partition " + partitionId + " from the meta Column Family", e);
        }
    }

    private static byte[] partitionIdKey(int partitionId) {
        assert partitionId >= 0 && partitionId <= 0xFFFF : partitionId;

        return ByteBuffer.allocate(PARTITION_ID_PREFIX.length + Short.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .put(PARTITION_ID_PREFIX)
                .putShort((short) partitionId)
                .array();
    }

    @Override
    public void close() throws Exception {
        metaCf.close();
    }
}
