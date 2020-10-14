package org.apache.ignite.internal.processors.query.h2.database;

import org.h2.engine.Session;

/**
 * Holds H2 Session during work with index tree.
 */
public class ThreadLocalSessionHolder {
    /** */
    private static final ThreadLocal<Session> session = new ThreadLocal<>();

    /** */
    public static void setSession(Session ses) {
        session.set(ses);
    }

    /** */
    public static Session getSession() {
        return session.get();
    }

    /** */
    public static void removeSession() {
        session.remove();
    }
}
