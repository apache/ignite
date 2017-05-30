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

import org.apache.ignite.internal.util.typedef.internal.U;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Statement cache.
 */
public class H2StatementCache extends LinkedHashMap<String, PreparedStatement> {
    /** */
    private int size;

    /** Last usage. */
    private volatile long lastUsage;

    /**
     * @param size Size.
     */
    H2StatementCache(int size) {
        super(size, (float)0.75, true);

        this.size = size;
    }

    /** {@inheritDoc} */
    @Override protected boolean removeEldestEntry(Map.Entry<String, PreparedStatement> eldest) {
        boolean rmv = size() > size;

        if (rmv) {
            PreparedStatement stmt = eldest.getValue();

            U.closeQuiet(stmt);
        }

        return rmv;
    }

    /**
     * The timestamp of the last usage of the cache.
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
