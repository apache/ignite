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
 * Message for {@link CacheWriteSynchronizationMode}.
 * Consistency between code-to-value and value-to-code conversions must be provided.
 */
public class CacheWriteSynchronizationModeMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 503;

    /** Write synchronization mode. */
    private CacheWriteSynchronizationMode val;

    /** Code. */
    @Order(0)
    private byte code = -1;

    /** Constructor. */
    public CacheWriteSynchronizationModeMessage() {
        // No-op.
    }

    /** Constructor. */
    public CacheWriteSynchronizationModeMessage(CacheWriteSynchronizationMode val) {
        this.val = val;
        code = code(val);
    }

    /** @return Code. */
    public byte code() {
        return code;
    }

    /** @param code Code. */
    public void code(byte code) {
        this.code = code;
        val = value(code);
    }

    /** @return Write synchronization mode. */
    public CacheWriteSynchronizationMode value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /**
     * @param val Write synchronization mode.
     * @return Code.
     */
    private byte code(@Nullable CacheWriteSynchronizationMode val) {
        if (val == null)
            return -1;

        switch (val) {
            case FULL_SYNC:
                return 0;

            case FULL_ASYNC:
                return 1;

            case PRIMARY_SYNC:
                return 2;

            default:
                throw new IllegalArgumentException("Unknown write synchronization mode value: " + val);
        }
    }

    /**
     * @param code Code.
     * @return Write synchronization mode or null.
     */
    @Nullable private CacheWriteSynchronizationMode value(byte code) {
        switch (code) {
            case -1:
                return null;

            case 0:
                return CacheWriteSynchronizationMode.FULL_SYNC;

            case 1:
                return CacheWriteSynchronizationMode.FULL_ASYNC;

            case 2:
                return CacheWriteSynchronizationMode.PRIMARY_SYNC;

            default:
                throw new IllegalArgumentException("Unknown write synchronization mode code: " + code);
        }
    }
}
