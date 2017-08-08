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

import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Statement cache.
 */
public class H2StatementCache {
    /** */
    private final StatementsMap stmts;

    /** */
    private final Lock readLock;

    /** */
    private final Lock writeLock;

    /**
     * @param size Size.
     */
    H2StatementCache(int size) {
        stmts = new StatementsMap(size);

        ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

        readLock = readWriteLock.readLock();
        writeLock = readWriteLock.writeLock();
    }

    /** */
    public H2CachedPreparedStatement get(String sql) {
        readLock.lock();

        try {
            return stmts.get(sql);
        }
        finally {
            readLock.unlock();
        }
    }

    /** */
    public void put(String sql, H2CachedPreparedStatement stmt) {
        readLock.lock();

        try {
            // put may invoke removeEldestEntry()
            stmts.put(sql, stmt);
        }
        finally {
            readLock.unlock();
        }
    }

    /** */
    public void removeExpired(long expirationTime) {
        writeLock.lock();

        try {
            Iterator<Map.Entry<String, H2CachedPreparedStatement>> it = stmts.entrySet().iterator();

            while (it.hasNext()) {
                H2CachedPreparedStatement stmt = it.next().getValue();

                // Entries are sorted by access-order,
                // so no point in advancing past the item that is not expired.
                if (stmt.lastUsage() < expirationTime)
                    break;

                if (stmt.shutdown(false)) {
                    it.remove();

                    U.closeQuiet(stmt.statement());
                }
            }
        }
        finally {
            writeLock.unlock();
        }
    }

    /** */
    public void close() {
        writeLock.lock();

        try {
            for (H2CachedPreparedStatement stmt: stmts.values()) {
                // TODO: we need to make sure nobody is using the statement, but do not want to wait.
                stmt.shutdown(true);

                U.closeQuiet(stmt.statement());
            }
        }
        finally {
            writeLock.unlock();
        }
    }

    /** */
    private class StatementsMap extends LinkedHashMap<String, H2CachedPreparedStatement> {
        /** */
        private int size;

        /** */
        private StatementsMap(int size) {
            super(size, (float)0.75, true);

            this.size = size;
        }

        /** {@inheritDoc} */
        @Override protected boolean removeEldestEntry(Map.Entry<String, H2CachedPreparedStatement> eldest) {
            boolean rmv = size() > size;

            if (rmv) {
                H2CachedPreparedStatement stmt = eldest.getValue();

                U.closeQuiet(stmt.statement());
            }

            return rmv;
        }
    }
}
