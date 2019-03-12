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

package org.apache.ignite.internal.processors.database;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheFreeListImplSelfTest extends FreeListTest {
    /**
     * @param pageSize Page size.
     */
    @Override protected Storable createTestStorable(int pageSize) {
        ThreadLocalRandom cur = ThreadLocalRandom.current();

        int keySize = cur.nextInt(pageSize * 3 / 2) + 10;
        int valSize = cur.nextInt(pageSize * 5 / 2) + 10;

        return new TestDataRow(keySize, valSize);
    }

    /**
     *
     */
    private static class TestDataRow implements CacheDataRow {
        /** */
        private long link;

        /** */
        private TestCacheObject key;

        /** */
        private TestCacheObject val;

        /** */
        private GridCacheVersion ver;

        /**
         * @param keySize Key size.
         * @param valSize Value size.
         */
        private TestDataRow(int keySize, int valSize) {
            key = new TestCacheObject(keySize);
            val = new TestCacheObject(valSize);
            ver = new GridCacheVersion(keySize, valSize, 1);
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject key() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void key(KeyCacheObject key) {
            this.key = (TestCacheObject)key;
        }

        /** {@inheritDoc} */
        @Override public CacheObject value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion version() {
            return ver;
        }

        /** {@inheritDoc} */
        @Override public long expireTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int size() throws IgniteCheckedException {
            int len = key().valueBytesLength(null);

            len += value().valueBytesLength(null) + CacheVersionIO.size(version(), false) + 8;

            return len + (cacheId() != 0 ? 4 : 0);
        }

        /** {@inheritDoc} */
        @Override public int headerSize() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return link;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            this.link = link;
        }

        /** {@inheritDoc} */
        @Override public int hash() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long newMvccCoordinatorVersion() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long newMvccCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int newMvccOperationCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public int mvccOperationCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public byte mvccTxState() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public byte newMvccTxState() {
            return 0;
        }
    }

    /**
     *
     */
    private static class TestCacheObject implements KeyCacheObject {
        /** */
        private byte[] data;

        /**
         * @param size Object size.
         */
        private TestCacheObject(int size) {
            data = new byte[size];

            Arrays.fill(data, (byte)size);
        }

        /** {@inheritDoc} */
        @Override public boolean internal() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void partition(int part) {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject copy(int part) {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public <T> T value(CacheObjectValueContext ctx, boolean cpy) {
            return (T)data;
        }

        /** {@inheritDoc} */
        @Override public byte[] valueBytes(CacheObjectValueContext ctx) throws IgniteCheckedException {
            return data;
        }

        /** {@inheritDoc} */
        @Override public int valueBytesLength(CacheObjectContext ctx) throws IgniteCheckedException {
            return data.length;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(ByteBuffer buf) throws IgniteCheckedException {
            buf.put(data);

            return true;
        }

        /** {@inheritDoc} */
        @Override public int putValue(long addr) throws IgniteCheckedException {
            PageUtils.putBytes(addr, 0, data);

            return data.length;
        }

        /** {@inheritDoc} */
        @Override public boolean putValue(ByteBuffer buf, int off, int len) throws IgniteCheckedException {
            buf.put(data, off, len);

            return true;
        }

        /** {@inheritDoc} */
        @Override public byte cacheObjectType() {
            return 42;
        }

        /** {@inheritDoc} */
        @Override public boolean isPlatformType() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public CacheObject prepareForCache(CacheObjectContext ctx) {
            assert false;

            return this;
        }

        /** {@inheritDoc} */
        @Override public void finishUnmarshal(CacheObjectValueContext ctx, ClassLoader ldr)
            throws IgniteCheckedException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public void prepareMarshal(CacheObjectValueContext ctx) throws IgniteCheckedException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            assert false;

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            assert false;

            return false;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            assert false;

            return 0;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            assert false;

            return 0;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            assert false;
        }
    }
}
