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

package org.apache.ignite.internal.util.nio;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Meta keys for {@link GridNioSession}.
 */
public enum GridNioSessionMetaKey {
    /** NIO parser state. */
    PARSER_STATE,

    /** SSL handler. */
    SSL_HANDLER,

    /** NIO operation (request type). */
    NIO_OPERATION,

    /** Last future. */
    LAST_FUT,

    /** Client marshaller. */
    MARSHALLER,

    /** Client marshaller ID. */
    MARSHALLER_ID,

    /** Message writer. */
    MSG_WRITER,

    /** SSL engine. */
    SSL_ENGINE,

    /** Ack closure. */
    ACK_CLOSURE;

    /** Maximum count of NIO session keys in system. */
    public static final int MAX_KEYS_CNT = 64;

    /** NIO session key generator. */
    private static final AtomicInteger keyGen = new AtomicInteger(GridNioSessionMetaKey.values().length);

    /**
     * Returns next NIO session key ordinal for non-existing enum value.
     * <p>
     * NOTE: Maximum count of NIO session keys in system is limited by {@link #MAX_KEYS_CNT}.
     *
     * @return NIO session key ordinal for non-existing enum value.
     */
    public static int nextUniqueKey() {
        int res = keyGen.getAndIncrement();

        if (res >= MAX_KEYS_CNT)
            throw new IllegalStateException("Maximum count of NIO session keys in system is limited by: " +
                MAX_KEYS_CNT);

        return res;
    }
}