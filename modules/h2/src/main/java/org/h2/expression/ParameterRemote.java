/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.io.IOException;
import java.sql.ResultSetMetaData;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * A client side (remote) parameter.
 */
public class ParameterRemote implements ParameterInterface {

    private Value value;
    private final int index;
    private int dataType = Value.UNKNOWN;
    private long precision;
    private int scale;
    private int nullable = ResultSetMetaData.columnNullableUnknown;

    public ParameterRemote(int index) {
        this.index = index;
    }

    @Override
    public void setValue(Value newValue, boolean closeOld) {
        if (closeOld && value != null) {
            value.remove();
        }
        value = newValue;
    }

    @Override
    public Value getParamValue() {
        return value;
    }

    @Override
    public void checkSet() {
        if (value == null) {
            throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    @Override
    public boolean isValueSet() {
        return value != null;
    }

    @Override
    public int getType() {
        return value == null ? dataType : value.getType();
    }

    @Override
    public long getPrecision() {
        return value == null ? precision : value.getPrecision();
    }

    @Override
    public int getScale() {
        return value == null ? scale : value.getScale();
    }

    @Override
    public int getNullable() {
        return nullable;
    }

    /**
     * Write the parameter meta data from the transfer object.
     *
     * @param transfer the transfer object
     */
    public void readMetaData(Transfer transfer) throws IOException {
        dataType = transfer.readInt();
        precision = transfer.readLong();
        scale = transfer.readInt();
        nullable = transfer.readInt();
    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param transfer the transfer object
     * @param p the parameter
     */
    public static void writeMetaData(Transfer transfer, ParameterInterface p)
            throws IOException {
        transfer.writeInt(p.getType());
        transfer.writeLong(p.getPrecision());
        transfer.writeInt(p.getScale());
        transfer.writeInt(p.getNullable());
    }

}
