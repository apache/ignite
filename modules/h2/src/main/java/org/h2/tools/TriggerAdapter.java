/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.h2.api.Trigger;

/**
 * An adapter for the trigger interface that allows to use the ResultSet
 * interface instead of a row array.
 */
public abstract class TriggerAdapter implements Trigger {

    /**
     * The schema name.
     */
    protected String schemaName;

    /**
     * The name of the trigger.
     */
    protected String triggerName;

    /**
     * The name of the table.
     */
    protected String tableName;

    /**
     * Whether the fire method is called before or after the operation is
     * performed.
     */
    protected boolean before;

    /**
     * The trigger type: INSERT, UPDATE, DELETE, SELECT, or a combination (a bit
     * field).
     */
    protected int type;

    private SimpleResultSet oldResultSet, newResultSet;
    private TriggerRowSource oldSource, newSource;

    /**
     * This method is called by the database engine once when initializing the
     * trigger. It is called when the trigger is created, as well as when the
     * database is opened. The default implementation initialized the result
     * sets.
     *
     * @param conn a connection to the database
     * @param schemaName the name of the schema
     * @param triggerName the name of the trigger used in the CREATE TRIGGER
     *            statement
     * @param tableName the name of the table
     * @param before whether the fire method is called before or after the
     *            operation is performed
     * @param type the operation type: INSERT, UPDATE, DELETE, SELECT, or a
     *            combination (this parameter is a bit field)
     */
    @Override
    public void init(Connection conn, String schemaName,
            String triggerName, String tableName,
            boolean before, int type) throws SQLException {
        ResultSet rs = conn.getMetaData().getColumns(
                null, schemaName, tableName, null);
        oldSource = new TriggerRowSource();
        newSource = new TriggerRowSource();
        oldResultSet = new SimpleResultSet(oldSource);
        newResultSet = new SimpleResultSet(newSource);
        while (rs.next()) {
            String column = rs.getString("COLUMN_NAME");
            int dataType = rs.getInt("DATA_TYPE");
            int precision = rs.getInt("COLUMN_SIZE");
            int scale = rs.getInt("DECIMAL_DIGITS");
            oldResultSet.addColumn(column, dataType, precision, scale);
            newResultSet.addColumn(column, dataType, precision, scale);
        }
        this.schemaName = schemaName;
        this.triggerName = triggerName;
        this.tableName = tableName;
        this.before = before;
        this.type = type;
    }

    /**
     * A row source that allows to set the next row.
     */
    static class TriggerRowSource implements SimpleRowSource {

        private Object[] row;

        void setRow(Object[] row) {
            this.row = row;
        }

        @Override
        public Object[] readRow() {
            return row;
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void reset() {
            // ignore
        }

    }

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
     * The default implementation calls the fire method with the ResultSet
     * parameters.
     * </p>
     *
     * @param conn a connection to the database
     * @param oldRow the old row, or null if no old row is available (for
     *            INSERT)
     * @param newRow the new row, or null if no new row is available (for
     *            DELETE)
     * @throws SQLException if the operation must be undone
     */
    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow)
            throws SQLException {
        fire(conn, wrap(oldResultSet, oldSource, oldRow),
                wrap(newResultSet, newSource, newRow));
    }

    /**
     * This method is called for each triggered action by the default
     * fire(Connection conn, Object[] oldRow, Object[] newRow) method.
     * ResultSet.next does not need to be called (and calling it has no effect;
     * it will always return true).
     * <p>
     * For "before" triggers, the new values of the new row may be changed
     * using the ResultSet.updateX methods.
     * </p>
     *
     * @param conn a connection to the database
     * @param oldRow the old row, or null if no old row is available (for
     *            INSERT)
     * @param newRow the new row, or null if no new row is available (for
     *            DELETE)
     * @throws SQLException if the operation must be undone
     */
    public abstract void fire(Connection conn, ResultSet oldRow,
            ResultSet newRow) throws SQLException;

    private static SimpleResultSet wrap(SimpleResultSet rs,
            TriggerRowSource source, Object[] row) throws SQLException {
        if (row == null) {
            return null;
        }
        source.setRow(row);
        rs.next();
        return rs;
    }

    /**
     * This method is called when the database is closed.
     * If the method throws an exception, it will be logged, but
     * closing the database will continue.
     * The default implementation does nothing.
     */
    @Override
    public void remove() throws SQLException {
        // do nothing by default
    }

    /**
     * This method is called when the trigger is dropped.
     * The default implementation does nothing.
     */
    @Override
    public void close() throws SQLException {
        // do nothing by default
    }

}
