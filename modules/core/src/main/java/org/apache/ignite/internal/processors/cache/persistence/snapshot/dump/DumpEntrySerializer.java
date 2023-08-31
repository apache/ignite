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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Serialization logic for dump.
 */
public class DumpEntrySerializer {
    /** */
    private ByteBuffer buf;

    /** Kernal context. */
    private @Nullable GridKernalContext cctx;

    /** Cache object processor. */
    private IgniteCacheObjectProcessor co;

    /** */
    public DumpEntrySerializer() {
        buf = ByteBuffer.allocate(100);
    }

    /** */
    public void kernalContext(GridKernalContext cctx) {
        this.cctx = cctx;
        co = cctx.cacheObjects();
    }

    /**
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
        buf.rewind();
        buf.limit(buf.capacity());

        int keySz = key.valueBytesLength(coCtx);
        int valSz = val.valueBytesLength(coCtx);
        int dataSz = /*cache ID*/Integer.BYTES +
            /*expire Time*/Long.BYTES +
            /*key*/keySz +
            /*value*/valSz;
        int rowSz = dataSz + Integer.BYTES/*extra bytes for row size*/;

        if (buf.capacity() < rowSz)
            buf = ByteBuffer.allocate(rowSz);

        buf.putInt(dataSz);
        buf.putInt(cache);
        buf.putLong(expireTime);

        if (!key.putValue(buf))
            throw new IgniteCheckedException("Can't write key");

        if (!val.putValue(buf))
            throw new IgniteCheckedException("Can't write value");

        assert buf.position() == rowSz;

        buf.rewind();
        buf.limit(rowSz);

        return buf;
    }

    /**
     * @param dumpFile File to read data from.
     * @param grp Cache group.
     * @return dump entry.
     */
    public DumpEntry read(FileIO dumpFile, int grp, int part) throws IOException, IgniteCheckedException {
        assert cctx != null : "Set kernalContext first";

        byte[] dataSzBytes = new byte[Integer.BYTES];

        int read = dumpFile.readFully(dataSzBytes, 0, dataSzBytes.length);

        if (read <= 0)
            return null;

        if (read != dataSzBytes.length)
            throw new IgniteException("Expected to read " + dataSzBytes.length + " bytes but read only " + read);

        int dataSz = U.bytesToInt(dataSzBytes, 0);

        if (buf.capacity() < dataSz)
            buf = ByteBuffer.allocate(dataSz);

        buf.rewind();
        buf.limit(dataSz);

        read = dumpFile.readFully(buf);

        if (read != dataSz)
            throw new IgniteException("Expected to read " + dataSz + " bytes but read only " + read);

        buf.rewind();

        int cache = buf.getInt();
        long expireTime = buf.getLong();

        int keySz = buf.getInt();

        byte keyType = buf.get();

        byte[] keyBytes = new byte[keySz];

        buf.get(keyBytes, 0, keyBytes.length);

        GridCacheContext<?, ?> cacheCtx = cctx.cache().cacheGroup(grp).shared().cacheContext(cache);

        CacheObjectContext coCtx = cacheCtx.cacheObjectContext();

        KeyCacheObject key = co.toKeyCacheObject(coCtx, keyType, keyBytes);

        if (key.partition() == -1)
            key.partition(part);

        int valSz = buf.getInt();
        byte valType = buf.get();
        byte[] valBytes = new byte[valSz];

        buf.get(valBytes, 0, valBytes.length);

        CacheObject val = co.toCacheObject(coCtx, valType, valBytes);

        return new DumpEntry() {
            @Override public int groupId() {
                return grp;
            }

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
}
