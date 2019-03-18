/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import org.h2.command.Prepared;
import org.h2.expression.Parameter;
import org.h2.value.ValueBytes;

/**
 * This class represents a prepared statement.
 */
@SuppressWarnings("unused")
public class H2Program extends H2Closable {

    /**
     * The prepared statement
     */
    protected final Prepared prepared;

    H2Program(Prepared prepared) {
        this.prepared = prepared;
    }

    /**
     * Set the specified parameter value.
     *
     * @param index the parameter index (0, 1,...)
     * @param value the new value
     */
    public void bindBlob(int index, byte[] value) {
        getParameter(index).setValue(ValueBytes.get(value));

    }

    /**
     * Set the specified parameter value.
     *
     * @param index the parameter index (0, 1,...)
     * @param value the new value
     */
    public void bindDouble(int index, double value) {
        // TODO
    }

    /**
     * Set the specified parameter value.
     *
     * @param index the parameter index (0, 1,...)
     * @param value the new value
     */
    public void bindLong(int index, long value) {
        // TODO
    }

    /**
     * Set the specified parameter to NULL.
     *
     * @param index the parameter index (0, 1,...)
     */
    public void bindNull(int index) {
        // TODO
    }

    /**
     * Set the specified parameter value.
     *
     * @param index the parameter index (0, 1,...)
     * @param value the new value
     */
    public void bindString(int index, String value) {
        // TODO
    }

    /**
     * Reset all parameter values.
     */
    public void clearBindings() {
        // TODO
    }

    /**
     * Close the statement.
     */
    public void close() {
        // TODO
    }

    /**
     * Get the unique id of this statement.
     *
     * @return the id
     */
    public final int getUniqueId() {
        return 0;
    }

    /**
     * TODO
     */
    @Override
    protected void onAllReferencesReleased() {
        // TODO
    }

    /**
     * TODO
     */
    @Override
    protected void onAllReferencesReleasedFromContainer() {
        // TODO
    }

    private Parameter getParameter(int index) {
        return prepared.getParameters().get(index);
    }

}
