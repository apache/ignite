/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.h2.engine.Mode;
import org.h2.message.DbException;

/**
 * Implementation of NULL. NULL is not a regular data type.
 */
public class ValueNull extends Value {

    /**
     * The main NULL instance.
     */
    public static final ValueNull INSTANCE = new ValueNull();

    /**
     * The precision of NULL.
     */
    static final int PRECISION = 1;

    /**
     * The display size of the textual representation of NULL.
     */
    static final int DISPLAY_SIZE = 4;

    private ValueNull() {
        // don't allow construction
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        return builder.append("NULL");
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_NULL;
    }

    @Override
    public int getValueType() {
        return NULL;
    }

    @Override
    public int getMemory() {
        // Singleton value
        return 0;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public boolean getBoolean() {
        return false;
    }

    @Override
    public Date getDate() {
        return null;
    }

    @Override
    public Time getTime() {
        return null;
    }

    @Override
    public Timestamp getTimestamp() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public short getShort() {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return null;
    }

    @Override
    public double getDouble() {
        return 0.0;
    }

    @Override
    public float getFloat() {
        return 0.0F;
    }

    @Override
    public int getInt() {
        return 0;
    }

    @Override
    public long getLong() {
        return 0;
    }

    @Override
    public InputStream getInputStream() {
        return null;
    }

    @Override
    public Reader getReader() {
        return null;
    }

    @Override
    protected Value convertTo(int type, Mode mode, Object column, ExtTypeInfo extTypeInfo) {
        return this;
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        throw DbException.throwInternalError("compare null");
    }

    @Override
    public boolean containsNull() {
        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Object getObject() {
        return null;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setNull(parameterIndex, Types.NULL);
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

}
