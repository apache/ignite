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

package org.apache.ignite.cache.store.cassandra.session.pool;

import java.lang.Thread.State;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import com.datastax.driver.core.Session;
import org.apache.ignite.cache.store.cassandra.session.CassandraSessionImpl;

/**
 * Cassandra driver sessions pool.
 */
public class SessionPool {
    /**
     * Monitors session pool and closes unused session.
     */
    private static class SessionMonitor extends Thread {
        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (true) {
                    try {
                        Thread.sleep(SLEEP_TIMEOUT);
                    }
                    catch (InterruptedException ignored) {
                        return;
                    }

                    List<Map.Entry<CassandraSessionImpl, IdleSession>> expiredSessions = new LinkedList<>();

                    int sessionsCnt;

                    synchronized (sessions) {
                        sessionsCnt = sessions.size();

                        for (Map.Entry<CassandraSessionImpl, IdleSession> entry : sessions.entrySet()) {
                            if (entry.getValue().expired())
                                expiredSessions.add(entry);
                        }

                        for (Map.Entry<CassandraSessionImpl, IdleSession> entry : expiredSessions)
                            sessions.remove(entry.getKey());
                    }

                    for (Map.Entry<CassandraSessionImpl, IdleSession> entry : expiredSessions)
                        entry.getValue().release();

                    // all sessions in the pool expired, thus we don't need additional thread to manage sessions in the pool
                    if (sessionsCnt == expiredSessions.size())
                        return;
                }
            }
            finally {
                release();
            }
        }
    }

    /** Sessions monitor sleep timeout. */
    private static final long SLEEP_TIMEOUT = 60000; // 1 minute.

    /** Sessions which were returned to pool. */
    private static final Map<CassandraSessionImpl, IdleSession> sessions = new HashMap<>();

    /** Singleton instance. */
    private static SessionMonitor monitorSingleton;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                release();
            }
        });
    }

    /**
     * Returns Cassandra driver session to sessions pool.
     *
     * @param cassandraSes Session wrapper.
     * @param driverSes Driver session.
     */
    public static void put(CassandraSessionImpl cassandraSes, Session driverSes, long expirationTimeout) {
        if (cassandraSes == null || driverSes == null)
            return;

        IdleSession old;

        synchronized (sessions) {
            old = sessions.put(cassandraSes, new IdleSession(driverSes, expirationTimeout));

            if (monitorSingleton == null || State.TERMINATED.equals(monitorSingleton.getState())) {
                monitorSingleton = new SessionMonitor();
                monitorSingleton.setDaemon(true);
                monitorSingleton.setName("Cassandra-sessions-pool");
                monitorSingleton.start();
            }
        }

        if (old != null)
            old.release();
    }

    /**
     * Extracts Cassandra driver session from pool.
     *
     * @param cassandraSes Session wrapper.
     * @return Cassandra driver session.
     */
    public static Session get(CassandraSessionImpl cassandraSes) {
        if (cassandraSes == null)
            return null;

        IdleSession wrapper;

        synchronized (sessions) {
            wrapper = sessions.remove(cassandraSes);
        }

        return wrapper == null ? null : wrapper.driverSession();
    }

    /**
     * Releases all session from pool and closes all their connections to Cassandra database.
     */
    public static void release() {
        Collection<IdleSession> wrappers;

        synchronized (sessions) {
            try {
                if (sessions.isEmpty())
                    return;

                wrappers = new LinkedList<>();

                for (IdleSession wrapper : sessions.values())
                    wrappers.add(wrapper);

                sessions.clear();
            }
            finally {
                if (!(Thread.currentThread() instanceof SessionMonitor) && monitorSingleton != null) {
                    try {
                        monitorSingleton.interrupt();
                    }
                    catch (Throwable ignored) {
                    }
                }
            }
        }

        for (IdleSession wrapper : wrappers)
            wrapper.release();
    }
}
