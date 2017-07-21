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

package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Hash multimap.
 */
public class HadoopHashMultimap extends HadoopHashMultimapBase {
    /** */
    private long[] tbl;

    /** */
    private int keys;

    /**
     * @param jobInfo Job info.
     * @param mem Memory.
     * @param cap Initial capacity.
     */
    public HadoopHashMultimap(HadoopJobInfo jobInfo, GridUnsafeMemory mem, int cap) {
        super(jobInfo, mem);

        assert U.isPow2(cap) : cap;

        tbl = new long[cap];
    }

    /** {@inheritDoc} */
    @Override public Adder startAdding(HadoopTaskContext ctx) throws IgniteCheckedException {
        return new AdderImpl(ctx);
    }

    /**
     * Rehash.
     */
    private void rehash() {
        long[] newTbl = new long[tbl.length << 1];

        int newMask = newTbl.length - 1;

        for (long meta : tbl) {
            while (meta != 0) {
                long collision = collision(meta);

                int idx = keyHash(meta) & newMask;

                collision(meta, newTbl[idx]);

                newTbl[idx] = meta;

                meta = collision;
            }
        }

        tbl = newTbl;
    }

    /**
     * @return Keys count.
     */
    public int keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return tbl.length;
    }

    /** {@inheritDoc} */
    @Override protected long meta(int idx) {
        return tbl[idx];
    }

    /**
     * Adder.
     */
    private class AdderImpl extends AdderBase {
        /** */
        private final Reader keyReader;

        /**
         * @param ctx Task context.
         * @throws IgniteCheckedException If failed.
         */
        protected AdderImpl(HadoopTaskContext ctx) throws IgniteCheckedException {
            super(ctx);

            keyReader = new Reader(keySer);
        }

        /**
         * @param keyHash Key hash.
         * @param keySize Key size.
         * @param keyPtr Key pointer.
         * @param valPtr Value page pointer.
         * @param collisionPtr Pointer to meta with hash collision.
         * @return Created meta page pointer.
         */
        private long createMeta(int keyHash, int keySize, long keyPtr, long valPtr, long collisionPtr) {
            long meta = allocate(32);

            mem.writeInt(meta, keyHash);
            mem.writeInt(meta + 4, keySize);
            mem.writeLong(meta + 8, keyPtr);
            mem.writeLong(meta + 16, valPtr);
            mem.writeLong(meta + 24, collisionPtr);

            return meta;
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) throws IgniteCheckedException {
            A.notNull(val, "val");

            int keyHash = U.hash(key.hashCode());

            // Write value.
            long valPtr = write(12, val, valSer);
            int valSize = writtenSize() - 12;

            valueSize(valPtr, valSize);

            // Find position in table.
            int idx = keyHash & (tbl.length - 1);

            long meta = tbl[idx];

            // Search for our key in collisions.
            while (meta != 0) {
                if (keyHash(meta) == keyHash && key.equals(keyReader.readKey(meta))) { // Found key.
                    nextValue(valPtr, value(meta));

                    value(meta, valPtr);

                    return;
                }

                meta = collision(meta);
            }

            // Write key.
            long keyPtr = write(0, key, keySer);
            int keySize = writtenSize();

            nextValue(valPtr, 0);

            tbl[idx] = createMeta(keyHash, keySize, keyPtr, valPtr, tbl[idx]);

            if (++keys > (tbl.length >>> 2) * 3)
                rehash();
        }
    }
}