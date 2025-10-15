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

import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class GridCacheOperationMessage implements Message {
    /** */
    public static final short TYPE_CODE = 190;

    /** */
    @Nullable private GridCacheOperation cacheOperation;

    /** */
    @Order(0)
    private byte code = -1;

    /** */
    public GridCacheOperationMessage() {
        // No-op.
    }

    /** */
    public GridCacheOperationMessage(GridCacheOperation cacheOperation) {
        this.cacheOperation = cacheOperation;
        this.code = encode(cacheOperation);
    }

    /** */
    private static byte encode(@Nullable GridCacheOperation operation) {
        if (operation == null)
            return -1;

        switch (operation) {
            case READ: return 0;
            case CREATE: return 1;
            case UPDATE: return 2;
            case DELETE: return 3;
            case TRANSFORM: return 4;
            case RELOAD: return 5;
            case NOOP: return 6;
        }

        throw new IllegalArgumentException("Unknown cache operation: " + operation);
    }

    /** */
    @Nullable private static GridCacheOperation decode(byte code) {
        switch (code) {
            case -1: return null;
            case 0: return GridCacheOperation.READ;
            case 1: return GridCacheOperation.CREATE;
            case 2: return GridCacheOperation.UPDATE;
            case 3: return GridCacheOperation.DELETE;
            case 4: return GridCacheOperation.TRANSFORM;
            case 5: return GridCacheOperation.RELOAD;
            case 6: return GridCacheOperation.NOOP;
        }

        throw new IllegalArgumentException("Unknown cache operation code: " + code);
    }

    /** */
    public void code(byte code) {
        this.code = code;
        this.cacheOperation = decode(code);
    }

    /** */
    public byte code() {
        return code;
    }

    /** */
    @Nullable public GridCacheOperation value() {
        return cacheOperation;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
