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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Prepared statement along with usage counter.
 */
public class H2CachedPreparedStatement {
    /** Prepared statement. */
    private final PreparedStatement stmt;

    /** Usage counter.
     *
     * Possible values:
     * 0 - free;
     * 1 - being used;
     * -1 - closed.
     * */
    private final AtomicInteger cntr = new AtomicInteger(1);

    /** Last usage. */
    private volatile long lastUsage;

    /**
     * Constructor.
     *
     * @param stmt Prepared statement.
     */
    public H2CachedPreparedStatement(PreparedStatement stmt) {
        this.stmt = stmt;

        updateLastUsage();
    }

    /**
     * @return Prepared statement.
     */
    public PreparedStatement statement() {
        return stmt;
    }

    /**
     * Acquire.
     *
     * @return {@code true} If succeeded.
     */
    public boolean acquire() {
        return cntr.compareAndSet(0, 1);
    }

    /**
     * Release.
     */
    public void release() {
        cntr.compareAndSet(1, 0);
    }

    /**
     * Ensures nobody uses the statement and prevents further usage.
     *
     * @param force Flag to skip the check.
     * @return {@code true} if succeeded.
     */
    public boolean shutdown(boolean force) {
        if (force) {
            cntr.set(-1);

            return true;
        }

        return cntr.compareAndSet(0, -1);
    }

    /**
     * The timestamp of the last usage of the connection.
     *
     * @return last usage timestamp
     */
    public long lastUsage() {
        return lastUsage;
    }

    /**
     * Updates the {@link #lastUsage} timestamp by current time.
     */
    public void updateLastUsage() {
        lastUsage = U.currentTimeMillis();
    }
}
