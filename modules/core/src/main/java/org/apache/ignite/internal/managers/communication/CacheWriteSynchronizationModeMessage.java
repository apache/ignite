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
import org.jetbrains.annotations.Nullable;

/** */
public class CacheWriteSynchronizationModeMessage extends EnumMessage<CacheWriteSynchronizationMode> {
    /** Type code. */
    public static final short TYPE_CODE = 503;

    /** Constructor. */
    public CacheWriteSynchronizationModeMessage() {
        // No-op.
    }

    /** Constructor. */
    public CacheWriteSynchronizationModeMessage(@Nullable CacheWriteSynchronizationMode val) {
        super(val);
    }

    /** {@inheritDoc} */
    @Override protected byte code0(CacheWriteSynchronizationMode val) {
        switch (val) {
            case FULL_SYNC: return 0;
            case FULL_ASYNC: return 1;
            case PRIMARY_SYNC: return 2;
        }

        throw new IllegalArgumentException("Unknown cache write synchronization mode: " + val);
    }

    /** {@inheritDoc} */
    @Override protected CacheWriteSynchronizationMode value0(byte code) {
        switch (code) {
            case 0: return CacheWriteSynchronizationMode.FULL_SYNC;
            case 1: return CacheWriteSynchronizationMode.FULL_ASYNC;
            case 2: return CacheWriteSynchronizationMode.PRIMARY_SYNC;
        }

        throw new IllegalArgumentException("Unknown cache write synchronization mode code: " + code);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
