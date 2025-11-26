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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class CacheWriteSynchronizationModeMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 503;

    /** Cache write synchronization mode value. */
    @Order(value = 0, asType = "byte", method = "code")
    @Nullable private CacheWriteSynchronizationMode cacheWriteSyncMode;

    /** Constructor. */
    public CacheWriteSynchronizationModeMessage() {
        // No-op.
    }

    /** Constructor. */
    public CacheWriteSynchronizationModeMessage(@Nullable CacheWriteSynchronizationMode mode) {
        cacheWriteSyncMode = mode;
    }

    /** */
    public byte code() {
        return cacheWriteSyncMode == null ? -1 : (byte)cacheWriteSyncMode.ordinal();
    }

    /** */
    public void code(byte mode) {
        cacheWriteSyncMode = CacheWriteSynchronizationMode.fromOrdinal(mode);
    }

    /** @param mode Cache write synchronization mode to encode. */
    private static byte encode(@Nullable CacheWriteSynchronizationMode mode) {
        if (mode == null)
            return -1;

        switch (mode) {
            case FULL_SYNC: return 0;
            case FULL_ASYNC: return 1;
            case PRIMARY_SYNC: return 2;
        }

        throw new IllegalArgumentException("Unknown cache write synchronization mode: " + mode);
    }

    /** @param code Code of cache write synchronization mode to decode. */
    @Nullable private static CacheWriteSynchronizationMode decode(short code) {
        switch (code) {
            case -1: return null;
            case 0: return CacheWriteSynchronizationMode.FULL_SYNC;
            case 1: return CacheWriteSynchronizationMode.FULL_ASYNC;
            case 2: return CacheWriteSynchronizationMode.PRIMARY_SYNC;
        }

        throw new IllegalArgumentException("Unknown cache write synchronization mode code: " + code);
    }

    /** @return Cache write synchronization mode value. */
    public CacheWriteSynchronizationMode value() {
        return cacheWriteSyncMode;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
