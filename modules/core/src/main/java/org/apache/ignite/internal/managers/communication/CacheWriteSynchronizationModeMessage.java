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

/**
 * Message of {@link CacheWriteSynchronizationMode}.
 * <p>
 * The values test is CacheWriteSynchroizationModeMessageTest.
 */
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

    /** @return Cache write synchronization mode value. */
    public CacheWriteSynchronizationMode value() {
        return cacheWriteSyncMode;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
