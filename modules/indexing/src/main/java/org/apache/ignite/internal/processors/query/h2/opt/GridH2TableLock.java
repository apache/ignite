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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.table.Table;
import org.h2.util.New;

/**
 * H2 Table lock.
 */
public class GridH2TableLock {
    /** Internal lock timeout. */
    public static final int INTERNAL_LOCK_TIMEOUT = 2000;

    /** Lock exclusive session. */
    private volatile IgniteSession lockExclusiveSession;

    /** Lock shared sessions. */
    private HashSet<IgniteSession> lockSharedSessions = New.hashSet();

    /** Mutex. */
    private Object mux = new Object();

    /**
     * The queue of sessions waiting to lock the table. It is a FIFO queue to
     * prevent starvation, since Java's synchronized locking is biased.
     */
    private final ArrayDeque<IgniteSession> waitingSessions = new ArrayDeque<>();

    /**
     * @param session Session.
     * @param table Table.
     * @param exclusive Exclusive lock.
     * @param msg Message.
     */
    private void traceLock(IgniteSession session, Table table, boolean exclusive, String msg) {
        System.out.println("Table lock: " + msg + ". [session=" +session.getClass().getSimpleName() + "#" +  session.getId()
            + ", exclusive=" + exclusive + ", table=" + table.getName() + ']');
    }

    /**
     * @return {@code true} if the table is locker exclusively.
     */
    public boolean isLockedExclusively() {
        return lockExclusiveSession != null;
    }

    /**
     * @param s Session
     * @return {@code true} if the table is locker exclusively by specified session.
     */
    public boolean isLockedExclusivelyBy(Session s) {
        return F.eq(new IgniteH2Session(s), lockExclusiveSession);
    }

    /**
     * @param s Session
     * @return {@code true} if the table is locker exclusively by specified session.
     */
    private boolean isLockedExclusivelyBy(IgniteSession s) {
        return F.eq(lockExclusiveSession, s);
    }

    /**
     * @param s Session.
     * @param table Table.
     */
    public void unlock(Session s, Table table) {
        unlock(new IgniteH2Session(s), table);
    }

    /**
     * @param table Table.
     */
    public void unlock(Table table) {
        unlock(new IgniteInternalSession(), table);
    }

    /**
     * @param s Session.
     * @param table Table.
     */
    private void unlock(IgniteSession s, Table table) {
        traceLock(s, table, F.eq(lockExclusiveSession, s), "unlock");

        synchronized (mux) {
            if (F.eq(lockExclusiveSession,  s))
                lockExclusiveSession = null;

            if (!lockSharedSessions.isEmpty())
                lockSharedSessions.remove(s);

            if (!waitingSessions.isEmpty())
                mux.notifyAll();
        }
    }

    /**
     * @param session Session.
     * @param table Table.
     * @param exclusive Exclusively lock flag.
     * @return {@code true} if locked.
     */
    public boolean lock(Session session, Table table, boolean exclusive) {
        return lock(new IgniteH2Session(session), table, exclusive);
    }

    /**
     * @param table Table.
     * @param exclusive Exclusively lock flag.
     * @return {@code true} if locked.
     */
    public boolean lock(Table table, boolean exclusive) {
        return lock(new IgniteInternalSession(), table, exclusive);
    }

    /**
     * @param session Session.
     * @param table Table.
     * @param exclusive Exclusively lock flag.
     * @return {@code true} if locked.
     */
    private boolean lock(IgniteSession session, Table table, boolean exclusive) {
            if (F.eq(lockExclusiveSession, session))
                return true;

        synchronized (mux) {
            if (F.eq(lockExclusiveSession, session))
                return true;

            if (!exclusive && lockSharedSessions.contains(session))
                return true;

            session.setWaitForLock(table, Thread.currentThread());

            waitingSessions.addLast(session);

            try {
                doLock1(session, table, exclusive);
            }
            finally {
                session.setWaitForLock(null, null);

                waitingSessions.remove(session);
            }
        }
        return false;
    }

    /**
     * @param session Session.
     * @param table Table.
     * @param exclusive {@code true} in case exclusive lock is requested.
     */
    private void doLock1(IgniteSession session, Table table, boolean exclusive) {
        traceLock(session, table, exclusive, "request");

        // don't get the current time unless necessary
        long max = 0;

        boolean checkDeadlock = false;

        while (true) {
            // if I'm the next one in the queue
            if (F.eq(waitingSessions.getFirst(), session)) {
                if (doLock2(session, table, exclusive))
                    return;
            }

            if (checkDeadlock && session instanceof IgniteH2Session) {
                ArrayList<Session> sessions = checkDeadlock(((IgniteH2Session)session).session(), null, null);
                if (sessions != null)
                    throw new IgniteException("Table deadlock is detected. " + getDeadlockDetails(sessions, exclusive));
            }
            else {
                // check for deadlocks from now on
                checkDeadlock = true;
            }
            long now = System.nanoTime();
            if (max == 0) {
                // try at least one more time
                max = now + TimeUnit.MILLISECONDS.toNanos(session.getLockTimeout());
            }
            else if (now >= max) {
                traceLock(session, table, exclusive, "timeout after " + session.getLockTimeout());
                throw new IgniteException("Table lock timeout [table=" + table.getName());
            }
            try {
                traceLock(session, table, exclusive, "waiting for");

                // don't wait too long so that deadlocks are detected early
                mux.wait(1);
            }
            catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * /**
     *
     * @param session Session.
     * @param table Table.
     * @param exclusive {@code true} in case exclusive lock is requested.
     * @return {@code true} if locked.
     */
    private boolean doLock2(IgniteSession session, Table table, boolean exclusive) {
        if (exclusive) {
            if (lockExclusiveSession == null) {
                if (lockSharedSessions.isEmpty()) {
                    traceLock(session, table, exclusive, "added for");

                    session.addLock(table);

                    lockExclusiveSession = session;

                    return true;
                }
                else if (lockSharedSessions.size() == 1 &&
                    lockSharedSessions.contains(session)) {

                    traceLock(session, table, exclusive, "add (upgraded) for ");

                    lockExclusiveSession = session;

                    return true;
                }
            }
        }
        else {
            if (lockExclusiveSession == null) {
                if (!lockSharedSessions.contains(session)) {
                    traceLock(session, table, exclusive, "ok");

                    session.addLock(table);

                    lockSharedSessions.add(session);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * @param session Session tries to lock.
     * @param clash Session to check deadlock.
     * @param visited Already checked sessions.
     * @return List of deadlocked sessions.
     */
    public ArrayList<Session> checkDeadlock(Session session, Session clash, Set<Session> visited) {
        // only one deadlock check at any given time
        synchronized (GridH2TableLock.class) {
            if (clash == null) {
                // verification is started
                clash = session;

                visited = New.hashSet();
            }
            else if (F.eq(clash, session)) {
                // we found a circle where this session is involved
                return New.arrayList();
            }
            else if (visited.contains(session)) {
                // we have already checked this session.
                // there is a circle, but the sessions in the circle need to
                // find it out themselves
                return null;
            }
            visited.add(session);

            ArrayList<Session> error = null;

            for (IgniteSession s : lockSharedSessions) {
                if (F.eq(s, session)) {
                    // it doesn't matter if we have locked the object already
                    continue;
                }

                Table t = s.getWaitForLock();

                if (t != null && s instanceof IgniteH2Session) {
                    error = t.checkDeadlock(((IgniteH2Session)s).session(), clash, visited);

                    if (error != null) {
                        error.add(session);

                        break;
                    }
                }
            }
            if (error == null && lockExclusiveSession != null) {
                Table t = lockExclusiveSession.getWaitForLock();

                if (t != null && lockExclusiveSession instanceof  IgniteH2Session) {
                    error = t.checkDeadlock(((IgniteH2Session)lockExclusiveSession).session(), clash, visited);

                    if (error != null)
                        error.add(session);
                }
            }
            return error;
        }
    }

    /**
     * @param sessions Sessions.
     * @param exclusive exclusive lock flag.
     * @return Detailed dealock message.
     */
    private static String getDeadlockDetails(ArrayList<Session> sessions, boolean exclusive) {
        // We add the thread details here to make it easier for customers to
        // match up these error messages with their own logs.
        StringBuilder buff = new StringBuilder();

        for (Session s : sessions) {
            Table lock = s.getWaitForLock();

            Thread thread = s.getWaitForLockThread();

            buff.append("\nSession ").
                append(s.toString()).
                append(" on thread ").
                append(thread.getName()).
                append(" is waiting to lock ").
                append(lock.toString()).
                append(exclusive ? " (exclusive)" : " (shared)").
                append(" while locking ");

            int i = 0;

            for (Table t : s.getLocks()) {
                if (i++ > 0)
                    buff.append(", ");

                buff.append(t.toString());

                if (t.isLockedExclusivelyBy(s))
                    buff.append(" (exclusive)");
                else
                    buff.append(" (shared)");
            }

            buff.append('.');
        }

        return buff.toString();
    }

    /**
     *
     */
    private interface IgniteSession {
        /**
         * @return Session ID.
         */
        int getId();

        /**
         * @param table Add lock for table.
         */
        void addLock(Table table);

        /**
         * @param waitForLock Table.
         * @param waitForLockThread Thread.
         */
        void setWaitForLock(Table waitForLock, Thread waitForLockThread);

        /**
         * @return Lock timeout.
         */
        int getLockTimeout();

        /**
         * @return Table that waits for lock by session.
         */
        Table getWaitForLock();
    }

    /**
     *
     */
    private static class IgniteInternalSession implements IgniteSession {
        /** Thread ID. */
        private long thId = Thread.currentThread().getId();

        /** Wait for lock. */
        private Table waitForLock;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            IgniteInternalSession session = (IgniteInternalSession)o;

            return thId == session.thId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(thId);
        }

        /** {@inheritDoc} */
        @Override public int getId() {
            return (int)thId;
        }

        /** {@inheritDoc} */
        @Override public void addLock(Table table) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setWaitForLock(Table waitForLock, Thread waitForLockThread) {
            this.waitForLock = waitForLock;
        }

        /** {@inheritDoc} */
        @Override public int getLockTimeout() {
            return INTERNAL_LOCK_TIMEOUT;
        }

        /** {@inheritDoc} */
        @Override public Table getWaitForLock() {
            return waitForLock;
        }
    }

    /**
     *
     */
    private static class IgniteH2Session implements IgniteSession {
        /** H2 session. */
        private final Session s;

        /**
         * @param s H2 session.
         */
        private IgniteH2Session(Session s) {
            this.s = s;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            IgniteH2Session session = (IgniteH2Session)o;

            return F.eq(s, session.s);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return s.hashCode();
        }

        /** {@inheritDoc} */
        @Override public int getId() {
            return s.getId();
        }

        /** {@inheritDoc} */
        @Override public void addLock(Table table) {
            s.addLock(table);
        }

        /** {@inheritDoc} */
        @Override public void setWaitForLock(Table waitForLock, Thread waitForLockThread) {
            s.setWaitForLock(waitForLock, waitForLockThread);
        }

        /** {@inheritDoc} */
        @Override public int getLockTimeout() {
            return s.getLockTimeout();
        }

        /** {@inheritDoc} */
        @Override public Table getWaitForLock() {
            return s.getWaitForLock();
        }

        /**
         * @return H2 session.
         */
        public Session session() {
            return s;
        }
    }
}