/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

/**
 * A class that implements this interface can create new database sessions. This
 * exists so that the JDBC layer (the client) can be compiled without dependency
 * to the core database engine.
 */
interface SessionFactory {

    /**
     * Create a new session.
     *
     * @param ci the connection parameters
     * @return the new session
     */
    SessionInterface createSession(ConnectionInfo ci) throws SQLException;

}
