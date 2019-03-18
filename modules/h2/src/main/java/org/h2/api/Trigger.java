/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A class that implements this interface can be used as a trigger.
 */
public interface Trigger {

    /**
     * The trigger is called for INSERT statements.
     */
    int INSERT = 1;

    /**
     * The trigger is called for UPDATE statements.
     */
    int UPDATE = 2;

    /**
     * The trigger is called for DELETE statements.
     */
    int DELETE = 4;

    /**
     * The trigger is called for SELECT statements.
     */
    int SELECT = 8;

    /**
     * This method is called by the database engine once when initializing the
     * trigger. It is called when the trigger is created, as well as when the
     * database is opened. The type of operation is a bit field with the
     * appropriate flags set. As an example, if the trigger is of type INSERT
     * and UPDATE, then the parameter type is set to (INSERT | UPDATE).
     *
     * @param conn a connection to the database (a system connection)
     * @param schemaName the name of the schema
     * @param triggerName the name of the trigger used in the CREATE TRIGGER
     *            statement
     * @param tableName the name of the table
     * @param before whether the fire method is called before or after the
     *            operation is performed
     * @param type the operation type: INSERT, UPDATE, DELETE, SELECT, or a
     *            combination (this parameter is a bit field)
     */
    void init(Connection conn, String schemaName, String triggerName,
            String tableName, boolean before, int type) throws SQLException;

    /**
     * This method is called for each triggered action. The method is called
     * immediately when the operation occurred (before it is committed). A
     * transaction rollback will also rollback the operations that were done
     * within the trigger, if the operations occurred within the same database.
     * If the trigger changes state outside the database, a rollback trigger
     * should be used.
     * <p>
     * The row arrays contain all columns of the table, in the same order
     * as defined in the table.
     * </p>
     * <p>
     * The trigger itself may change the data in the newRow array.
     * </p>
     *
     * @param conn a connection to the database
     * @param oldRow the old row, or null if no old row is available (for
     *            INSERT)
     * @param newRow the new row, or null if no new row is available (for
     *            DELETE)
     * @throws SQLException if the operation must be undone
     */
    void fire(Connection conn, Object[] oldRow, Object[] newRow)
            throws SQLException;

    /**
     * This method is called when the database is closed.
     * If the method throws an exception, it will be logged, but
     * closing the database will continue.
     */
    void close() throws SQLException;

    /**
     * This method is called when the trigger is dropped.
     */
    void remove() throws SQLException;

}
