/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

/**
 * This interface contains additional methods for database exceptions.
 */
public interface JdbcException {

    /**
     * Returns the H2-specific error code.
     *
     * @return the H2-specific error code
     */
    public int getErrorCode();

    /**
     * INTERNAL
     */
    String getOriginalMessage();

    /**
     * Returns the SQL statement.
     * <p>
     * SQL statements that contain '--hide--' are not listed.
     * </p>
     *
     * @return the SQL statement
     */
    String getSQL();

    /**
     * INTERNAL
     */
    void setSQL(String sql);

    /**
     * Returns the class name, the message, and in the server mode, the stack
     * trace of the server
     *
     * @return the string representation
     */
    @Override
    String toString();

}
