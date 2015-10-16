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

package org.apache.ignite.cache.store.cassandra.utils.session.pool;

import com.datastax.driver.core.Session;
import java.lang.Thread.State;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.store.cassandra.utils.session.CassandraSessionImpl;

/**
 * Cassandra driver sessions pool
 */
public class SessionPool {
    private static class SessionMonitor extends Thread {
        @Override public void run() {
            try {
                while (true) {
                    try {
                        Thread.sleep(SLEEP_TIMEOUT);
                    }
                    catch (InterruptedException ignored) {
                        return;
                    }

                    List<Map.Entry<CassandraSessionImpl, SessionWrapper>> expiredSessions = new LinkedList<>();

                    int sessionsCount;

                    synchronized (sessions) {
                        sessionsCount = sessions.size();

                        for (Map.Entry<CassandraSessionImpl, SessionWrapper> entry : sessions.entrySet()) {
                            if (entry.getValue().expired())
                                expiredSessions.add(entry);
                        }

                        for (Map.Entry<CassandraSessionImpl, SessionWrapper> entry : expiredSessions)
                            sessions.remove(entry.getKey());
                    }

                    for (Map.Entry<CassandraSessionImpl, SessionWrapper> entry : expiredSessions)
                        entry.getValue().release();

                    // all sessions in the pool expired, thus we don't need additional thread to manage sessions in the pool
                    if (sessionsCount == expiredSessions.size())
                        return;
                }
            }
            finally {
                release();
            }
        }
    }

    private static final long SLEEP_TIMEOUT = 60000; // 1 minute

    private static final Map<CassandraSessionImpl, SessionWrapper> sessions = new HashMap<>();

    private static SessionMonitor monitorSingleton;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                release();
            }
        });
    }

    public static void put(CassandraSessionImpl cassandraSession, Session driverSession) {
        if (cassandraSession == null || driverSession == null)
            return;

        SessionWrapper old;

        synchronized (sessions) {
            old = sessions.put(cassandraSession, new SessionWrapper(driverSession));

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

    public static Session get(CassandraSessionImpl cassandraSession) {
        if (cassandraSession == null)
            return null;

        SessionWrapper wrapper;

        synchronized (sessions) {
            wrapper = sessions.remove(cassandraSession);
        }

        return wrapper == null ? null : wrapper.driverSession();
    }

    public static void release() {
        Collection<SessionWrapper> wrappers;

        synchronized (sessions) {
            try {
                if (sessions.size() == 0)
                    return;

                wrappers = new LinkedList<>();

                for (SessionWrapper wrapper : sessions.values())
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

        for (SessionWrapper wrapper : wrappers)
            wrapper.release();
    }
}
