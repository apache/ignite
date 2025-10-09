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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class CacheWriteSynchronizationModeMessage implements Message {
    /** */
    public static final short TYPE_CODE = 190;

    /** */
    private CacheWriteSynchronizationMode cacheWriteSyncMode;

    /** */
    @Order(0)
    private short code = -1;

    /** */
    public CacheWriteSynchronizationModeMessage() {
        // No-op.
    }

    /** */
    public CacheWriteSynchronizationModeMessage(CacheWriteSynchronizationMode cacheWriteSyncMode) {
        this.cacheWriteSyncMode = cacheWriteSyncMode;
        this.code = encode(cacheWriteSyncMode);
    }

    /** */
    private static short encode(CacheWriteSynchronizationMode cacheWriteSyncMode) {
        switch (cacheWriteSyncMode) {
            case FULL_SYNC: return 0;
            case FULL_ASYNC: return 1;
            case PRIMARY_SYNC: return 2;
        }

        return -1;
    }

    /** */
    @Nullable private static CacheWriteSynchronizationMode decode(short code) {
        switch (code) {
            case 0: return CacheWriteSynchronizationMode.FULL_SYNC;
            case 1: return CacheWriteSynchronizationMode.FULL_ASYNC;
            case 2: return CacheWriteSynchronizationMode.PRIMARY_SYNC;
        }

        return null;
    }

    /** */
    public void code(short code) {
        this.code = code;
        this.cacheWriteSyncMode = decode(code);
    }

    /** */
    public short code() {
        return code;
    }

    /** */
    public CacheWriteSynchronizationMode cacheWriteSyncMode() {
        return cacheWriteSyncMode;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
