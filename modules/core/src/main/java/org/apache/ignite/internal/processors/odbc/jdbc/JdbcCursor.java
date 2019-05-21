/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JDBC Cursor.
 */
public abstract class JdbcCursor implements Closeable {
    /** Cursor Id generator. */
    private static final AtomicLong CURSOR_ID_GENERATOR = new AtomicLong();

    /** Cursor Id. */
    private final long cursorId;

    /** Id of the request that created given cursor. */
    private final long reqId;

    /**
     * Constructor.
     *
     * @param reqId Id of the request that created given cursor.
     */
    protected JdbcCursor(long reqId) {
        cursorId = CURSOR_ID_GENERATOR.getAndIncrement();

        this.reqId = reqId;
    }

    /**
     * @return Cursor Id.
     */
    public long cursorId() {
        return cursorId;
    }

    /**
     * @return Id of the request that created given cursor.
     */
    public long requestId() {
        return reqId;
    }
}
