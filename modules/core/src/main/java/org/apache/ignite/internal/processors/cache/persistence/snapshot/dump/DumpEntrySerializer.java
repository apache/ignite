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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Serialization logic for dump.
 */
public class DumpEntrySerializer {
    /** sizeOf(CRC) + sizeOf(Data size)  */
    public static final int HEADER_SZ = Integer.BYTES + Integer.BYTES;

    /** Default buffer allocator. */
    private static final Function<Long, ByteBuffer> DFLT_BUF_ALLOC = k -> ByteBuffer.allocate(100);

    /** */
    private final ConcurrentMap<Long, ByteBuffer> thLocBufs;

    /** */
    private final @Nullable ConcurrentMap<Long, ByteBuffer> encThLocBufs;

    /** */
    private final @Nullable EncryptionSpi encSpi;

    /** */
    private final @Nullable Serializable encKey;

    /** */
    private final FastCrc crc = new FastCrc();

    /** Cache object processor. */
    private IgniteCacheObjectProcessor co;

    /** Fake context. */
    private CacheObjectContext fakeCacheObjCtx;

    /** If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}. */
    private boolean raw;

    /** If {@code true} then return data in form of {@link BinaryObject}. */
    private boolean keepBinary;

    /**
     * @param thLocBufs Thread local buffers.
     * @param encKey Encrytpion key. If {@code null} then encryption disabled.
     * @param encSpi Encryption SPI to use.
     */
    public DumpEntrySerializer(
        ConcurrentMap<Long, ByteBuffer> thLocBufs,
        @Nullable ConcurrentMap<Long, ByteBuffer> encThLocBufs,
        @Nullable Serializable encKey,
        @Nullable EncryptionSpi encSpi
    ) {
        this.thLocBufs = thLocBufs;
        this.encThLocBufs = encThLocBufs;
        this.encKey = encKey;
        this.encSpi = encSpi;
    }

    /** */
    public void kernalContext(GridKernalContext cctx) {
        co = cctx.cacheObjects();
        fakeCacheObjCtx = new CacheObjectContext(cctx, null, null, false, false, false, false, false);
    }

    /** @param keepBinary If {@code true} then return data in form of {@link BinaryObject}. */
    public void keepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;
    }

    /** @param raw If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}. */
    public void raw(boolean raw) {
        this.raw = raw;
    }

    /**
     * @param cache Cache id.
     * @param expireTime Expire time.
     * @param key Key.
     * @param val Value.
     * @param coCtx Cache object context.
     * @return Buffer with serialized entry.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer writeToBuffer(
        int cache,
        long expireTime,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        CacheObjectContext coCtx
    ) throws IgniteCheckedException {
        ByteBuffer plainBuf = writeToBufferPlain(cache, expireTime, key, val, ver, coCtx);

        if (encKey == null)
            return plainBuf;

        int encDataSz = Integer.BYTES + encSpi.encryptedSize(plainBuf.limit());

        ByteBuffer encBuf = threadLocalBuffer(encThLocBufs);

        if (encBuf.capacity() < encDataSz)
            encBuf = enlargeThreadLocalBuffer(encThLocBufs, encDataSz);
        else
            encBuf.rewind().limit(encDataSz);

        encBuf.putInt(encDataSz - Integer.BYTES);
        encSpi.encrypt(plainBuf, encKey, encBuf);

        encBuf.position(0);

        return encBuf;
    }

    /**
     * Dump entry structure:
     * <pre>
     * +-----------+---------+----------+-----------------+-----+-----+-------+
     * | 4 bytes   | 4 bytes | 4 bytes  | 8 bytes         |     |     |       |
     * +-----------+---------+----------+-----------------+-----+-----+-------+
     * | Data size | CRC     | cache ID | expiration time | ver | key | value |
     * +-----------+---------+----------+-----------------+-----+-----+-------+
     * </pre>
     *
     * @param cache Cache id.
     * @param expireTime Expire time.
     * @param key Key.
     * @param val Value.
     * @param coCtx Cache object context.
     * @return Buffer with serialized entry.
     * @throws IgniteCheckedException If failed.
     */
    private ByteBuffer writeToBufferPlain(
        int cache,
        long expireTime,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        CacheObjectContext coCtx
    ) throws IgniteCheckedException {
        int dataSz = plainDataSize(key, val, ver, coCtx);
        int bufSz = dataSz + Integer.BYTES;

        ByteBuffer buf = threadLocalBuffer(thLocBufs);

        if (buf.capacity() < bufSz)
            buf = enlargeThreadLocalBuffer(thLocBufs, bufSz);
        else
            buf.rewind().limit(bufSz);

        buf.putInt(dataSz);
        buf.putInt(-1); // CRC value.
        buf.putInt(cache);
        buf.putLong(expireTime);

        boolean hasConflictVer = ver.otherClusterVersion() != null;

        buf.put((byte)(hasConflictVer ? 1 : 0));
        buf.putInt(ver.topologyVersion());
        buf.putLong(ver.order());
        buf.putInt(ver.nodeOrderAndDrIdRaw());

        if (hasConflictVer) {
            GridCacheVersion ver0 = (GridCacheVersion)ver.otherClusterVersion();

            buf.putInt(ver0.topologyVersion());
            buf.putLong(ver0.order());
            buf.putInt(ver0.nodeOrderAndDrIdRaw());
        }

        if (!key.putValue(buf))
            throw new IgniteCheckedException("Can't write key");

        if (!val.putValue(buf))
            throw new IgniteCheckedException("Can't write value");

        assert buf.position() == bufSz;

        buf.position(HEADER_SZ);

        crc.reset();
        crc.update(buf, dataSz - Integer.BYTES);

        buf.position(Integer.BYTES);
        buf.putInt(crc.getValue());

        buf.position(0);

        return buf;
    }

    /** */
    private static int plainDataSize(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        CacheObjectContext coCtx
    ) throws IgniteCheckedException {
        int verSz = Integer.BYTES/*topVer*/ + Long.BYTES/*order*/ + Integer.BYTES/*nodeOrderDrId*/;

        boolean hasConflictVer = ver.otherClusterVersion() != null;

        if (hasConflictVer)
            verSz *= 2 /*GridCacheVersion otherClusterVersion*/;

        int keySz = key.valueBytesLength(coCtx);
        int valSz = val.valueBytesLength(coCtx);

        return /* CRC */Integer.BYTES + /*cache ID*/Integer.BYTES + /*expire time*/Long.BYTES
            + /* hasConflictVersion */1 + /*version*/verSz + /*key*/keySz + /*value*/valSz;
    }

    /**
     * @param dumpFile File to read data from.
     * @return dump entry.
     */
    public DumpEntry read(FileIO dumpFile, int part) throws IOException, IgniteCheckedException {
        assert co != null : "Set kernalContext first";

        ByteBuffer buf = readRecord(dumpFile);

        if (buf == null)
            return null;

        if (encSpi != null) {
            ByteBuffer plainBuf = threadLocalBuffer(encThLocBufs);

            plainBuf.limit(plainBuf.capacity());

            if (plainBuf.capacity() < buf.limit())
                plainBuf = enlargeThreadLocalBuffer(encThLocBufs, buf.limit());
            else
                plainBuf.rewind();

            buf.position(Integer.BYTES);

            encSpi.decryptNoPadding(buf, encKey, plainBuf);

            plainBuf.rewind();

            int plainDataSz = plainBuf.getInt();

            plainBuf.limit(plainDataSz + Integer.BYTES);

            buf = plainBuf;
        }

        return plainDataEntry(part, buf);
    }

    /** */
    private ByteBuffer readRecord(FileIO file) throws IOException {
        ByteBuffer buf = threadLocalBuffer(thLocBufs);

        buf.position(0);
        buf.limit(Integer.BYTES);

        int read = file.readFully(buf);

        if (read < Integer.BYTES)
            return null;

        buf.position(0);

        int dataSz = buf.getInt();
        int bufSz = dataSz + Integer.BYTES;

        if (buf.capacity() < bufSz) {
            buf = enlargeThreadLocalBuffer(thLocBufs, bufSz);
            buf.position(Integer.BYTES);
        }
        else
            buf.limit(bufSz);

        read = file.readFully(buf);

        if (read != buf.limit() - Integer.BYTES)
            throw new IgniteException("Expected to read " + (buf.limit() - Integer.BYTES) + " bytes but read only " + read);

        return buf;
    }

    /** */
    private DumpEntry plainDataEntry(int part, ByteBuffer buf) throws IgniteCheckedException {
        buf.position(Integer.BYTES);

        int crc = buf.getInt();

        checkCRC(crc, buf);

        buf.position(HEADER_SZ);

        int cache = buf.getInt();
        long expireTime = buf.getLong();

        GridCacheVersion ver = readVersion(buf);

        int keySz = buf.getInt();

        byte keyType = buf.get();

        byte[] keyBytes = new byte[keySz];

        buf.get(keyBytes, 0, keyBytes.length);

        KeyCacheObject key = co.toKeyCacheObject(fakeCacheObjCtx, keyType, keyBytes);

        if (key.partition() == -1)
            key.partition(part);

        int valSz = buf.getInt();
        byte valType = buf.get();
        byte[] valBytes = new byte[valSz];

        buf.get(valBytes, 0, valBytes.length);

        CacheObject val = co.toCacheObject(fakeCacheObjCtx, valType, valBytes);

        return new DumpEntry() {
            @Override public int cacheId() {
                return cache;
            }

            @Override public long expireTime() {
                return expireTime;
            }

            @Override public CacheEntryVersion version() {
                return ver;
            }

            @Override public Object key() {
                return raw ? key : UnwrapDataEntry.unwrapKey(key, keepBinary, fakeCacheObjCtx);
            }

            @Override public Object value() {
                return raw ? val : UnwrapDataEntry.unwrapValue(val, keepBinary, fakeCacheObjCtx);
            }
        };
    }

    /** @return Written entry version. */
    private static GridCacheVersion readVersion(ByteBuffer buf) {
        boolean hasConflictVer = buf.get() == 1;

        int topVer = buf.getInt();
        long order = buf.getLong();
        int nodeOrderDrId = buf.getInt();

        if (!hasConflictVer)
            return new GridCacheVersion(topVer, nodeOrderDrId, order);

        int topVer0 = buf.getInt();
        long order0 = buf.getLong();
        int nodeOrderDrId0 = buf.getInt();

        return new GridCacheVersionEx(topVer, nodeOrderDrId, order, new GridCacheVersion(topVer0, nodeOrderDrId0, order0));
    }

    /** @return Thread local buffer. */
    private ByteBuffer threadLocalBuffer(ConcurrentMap<Long, ByteBuffer> map) {
        ByteBuffer res = map.computeIfAbsent(Thread.currentThread().getId(), DFLT_BUF_ALLOC);

        res.limit(res.capacity());
        res.position(0);

        return res;
    }

    /** @return Thread local buffer. */
    private ByteBuffer enlargeThreadLocalBuffer(ConcurrentMap<Long, ByteBuffer> map, int sz) {
        ByteBuffer buf = ByteBuffer.allocate(sz);

        map.put(Thread.currentThread().getId(), buf);

        return buf;
    }

    /** */
    private void checkCRC(int expCrc, ByteBuffer buf) {
        crc.reset();
        crc.update(buf, buf.limit() - buf.position());

        if (expCrc != crc.getValue())
            throw new IgniteException("Data corrupted [expCrc=" + expCrc + ", crc=" + crc + ']');
    }
}
