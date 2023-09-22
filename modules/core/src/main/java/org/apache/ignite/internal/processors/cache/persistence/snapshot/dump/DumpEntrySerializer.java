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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * Serialization logic for dump.
 */
public class DumpEntrySerializer {
    /** sizeOf(CRC) + sizeOf(Data size)  */
    public static final int HEADER_SZ = Integer.BYTES + Integer.BYTES;

    /** */
    private final ConcurrentMap<Long, ByteBuffer> thLocBufs;

    /** */
    private final FastCrc crc = new FastCrc();

    /** Kernal context. */
    private @Nullable GridKernalContext cctx;

    /** Cache object processor. */
    private IgniteCacheObjectProcessor co;

    /**
     * @param thLocBufs Thread local buffers.
     */
    public DumpEntrySerializer(ConcurrentMap<Long, ByteBuffer> thLocBufs) {
        this.thLocBufs = thLocBufs;
    }

    /** */
    public void kernalContext(GridKernalContext cctx) {
        this.cctx = cctx;
        co = cctx.cacheObjects();
    }

    /**
     * Dump entry structure:
     * <pre>
     * +---------+-----------+----------+-----------------+-----+-------+
     * | 4 bytes | 4 bytes   | 4 bytes  | 8 bytes         |     |       |
     * +---------+-----------+----------+-----------------+-----+-------+
     * | CRC     | Data size | cache ID | expiration time | key | value |
     * +---------+-----------+----------+-----------------+-----+-------+
     * </pre>
     *
     * @param cache Cache id.
     * @param expireTime Expire time.
     * @param key Key.
     * @param val Value.
     * @param coCtx Cache object context.
     * @return Buffer with serialized entry.
     * @throws IgniteCheckedException If failed
     */
    public ByteBuffer writeToBuffer(
        int cache,
        long expireTime,
        KeyCacheObject key,
        CacheObject val,
        CacheObjectContext coCtx
    ) throws IgniteCheckedException {
        int keySz = key.valueBytesLength(coCtx);
        int valSz = val.valueBytesLength(coCtx);
        int dataSz = /*cache ID*/Integer.BYTES + /*expire time*/Long.BYTES + /*key*/keySz + /*value*/valSz;

        int fullSz = dataSz + /*extra bytes for row size*/Integer.BYTES + /*CRC*/Integer.BYTES;

        ByteBuffer buf = threadLocalBuffer();

        if (buf.capacity() < fullSz)
            buf = ByteBuffer.allocate(fullSz);
        else
            buf.limit(fullSz);

        buf.position(Integer.BYTES); // CRC value.
        buf.putInt(dataSz);
        buf.putInt(cache);
        buf.putLong(expireTime);

        if (!key.putValue(buf))
            throw new IgniteCheckedException("Can't write key");

        if (!val.putValue(buf))
            throw new IgniteCheckedException("Can't write value");

        assert buf.position() == fullSz;

        buf.position(Integer.BYTES);

        crc.reset();
        crc.update(buf, fullSz - Integer.BYTES);

        buf.position(0);
        buf.putInt(crc.getValue());

        buf.position(0);

        return buf;
    }

    /**
     * @param dumpFile File to read data from.
     * @param grp Cache group.
     * @return dump entry.
     */
    public DumpEntry read(FileIO dumpFile, int grp, int part) throws IOException, IgniteCheckedException {
        assert cctx != null : "Set kernalContext first";

        ByteBuffer buf = threadLocalBuffer();

        buf.position(0);
        buf.limit(HEADER_SZ);

        int read = dumpFile.readFully(buf);

        if (read < HEADER_SZ)
            return null;

        buf.position(0);

        int crc = buf.getInt();
        int dataSz = buf.getInt();

        if (buf.capacity() < dataSz + HEADER_SZ) {
            buf = ByteBuffer.allocate(dataSz + HEADER_SZ);

            buf.position(HEADER_SZ - Integer.BYTES);
            buf.putInt(dataSz); // Required for CRC check.
        }
        else
            buf.limit(dataSz + HEADER_SZ);

        read = dumpFile.readFully(buf);

        if (read != dataSz)
            throw new IgniteException("Expected to read " + dataSz + " bytes but read only " + read);

        buf.position(HEADER_SZ - Integer.BYTES);

        checkCRC(crc, dataSz, buf);

        buf.position(HEADER_SZ);

        int cache = buf.getInt();
        long expireTime = buf.getLong();

        int keySz = buf.getInt();

        byte keyType = buf.get();

        byte[] keyBytes = new byte[keySz];

        buf.get(keyBytes, 0, keyBytes.length);

        GridCacheContext<?, ?> cacheCtx = Objects.requireNonNull(
            cctx.cache().cacheGroup(grp).shared().cacheContext(cache),
            "Can't find cache context!"
        );

        CacheObjectContext coCtx = Objects.requireNonNull(cacheCtx.cacheObjectContext(), "Can't find cache object context!");

        KeyCacheObject key = co.toKeyCacheObject(coCtx, keyType, keyBytes);

        if (key.partition() == -1)
            key.partition(part);

        int valSz = buf.getInt();
        byte valType = buf.get();
        byte[] valBytes = new byte[valSz];

        buf.get(valBytes, 0, valBytes.length);

        CacheObject val = co.toCacheObject(coCtx, valType, valBytes);

        return new DumpEntry() {
            @Override public int cacheId() {
                return cache;
            }

            @Override public long expireTime() {
                return expireTime;
            }

            @Override public KeyCacheObject key() {
                return key;
            }

            @Override public CacheObject value() {
                return val;
            }
        };
    }

    /** @return Thread local buffer. */
    private ByteBuffer threadLocalBuffer() {
        return thLocBufs.computeIfAbsent(Thread.currentThread().getId(), id -> ByteBuffer.allocate(100));
    }

    /** */
    private void checkCRC(int expCrc, int dataSz, ByteBuffer buf) {
        crc.reset();
        crc.update(buf, dataSz + Integer.BYTES /*dataSz field included in CRC calculation*/);

        if (expCrc != crc.getValue())
            throw new IgniteException("Data corrupted [expCrc=" + expCrc + ", crc=" + crc + ']');
    }
}
