/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.util.Arrays;
import org.h2.engine.SessionInterface;
import org.h2.util.MathUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueString;

/**
 * Result with padded fixed length strings.
 */
public class ResultWithPaddedStrings implements ResultInterface {

    private final ResultInterface source;

    /**
     * Returns wrapped result if necessary, or original result if it does not
     * contain visible CHAR columns.
     *
     * @param source
     *            source result
     * @return wrapped result or original result
     */
    public static ResultInterface get(ResultInterface source) {
        int count = source.getVisibleColumnCount();
        for (int i = 0; i < count; i++) {
            if (source.getColumnType(i).getValueType() == Value.STRING_FIXED) {
                return new ResultWithPaddedStrings(source);
            }
        }
        return source;
    }

    /**
     * Creates new instance of result.
     *
     * @param source
     *            the source result
     */
    private ResultWithPaddedStrings(ResultInterface source) {
        this.source = source;
    }

    @Override
    public void reset() {
        source.reset();
    }

    @Override
    public Value[] currentRow() {
        int count = source.getVisibleColumnCount();
        Value[] row = Arrays.copyOf(source.currentRow(), count);
        for (int i = 0; i < count; i++) {
            TypeInfo type = source.getColumnType(i);
            if (type.getValueType() == Value.STRING_FIXED) {
                long precision = type.getPrecision();
                if (precision == Integer.MAX_VALUE) {
                    // CHAR is CHAR(1)
                    precision = 1;
                }
                String s = row[i].getString();
                if (s != null && s.length() < precision) {
                    /*
                     * Use ValueString to avoid truncation of spaces. There is
                     * no difference between ValueStringFixed and ValueString
                     * for JDBC layer anyway.
                     */
                    row[i] = ValueString.get(rightPadWithSpaces(s, MathUtils.convertLongToInt(precision)));
                }
            }
        }
        return row;
    }

    private static String rightPadWithSpaces(String s, int length) {
        int used = s.length();
        if (length <= used) {
            return s;
        }
        char[] res = new char[length];
        s.getChars(0, used, res, 0);
        Arrays.fill(res, used, length, ' ');
        return new String(res);
    }

    @Override
    public boolean next() {
        return source.next();
    }

    @Override
    public int getRowId() {
        return source.getRowId();
    }

    @Override
    public boolean isAfterLast() {
        return source.isAfterLast();
    }

    @Override
    public int getVisibleColumnCount() {
        return source.getVisibleColumnCount();
    }

    @Override
    public int getRowCount() {
        return source.getRowCount();
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public boolean needToClose() {
        return source.needToClose();
    }

    @Override
    public void close() {
        source.close();
    }

    @Override
    public String getAlias(int i) {
        return source.getAlias(i);
    }

    @Override
    public String getSchemaName(int i) {
        return source.getSchemaName(i);
    }

    @Override
    public String getTableName(int i) {
        return source.getTableName(i);
    }

    @Override
    public String getColumnName(int i) {
        return source.getColumnName(i);
    }

    @Override
    public TypeInfo getColumnType(int i) {
        return source.getColumnType(i);
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return source.isAutoIncrement(i);
    }

    @Override
    public int getNullable(int i) {
        return source.getNullable(i);
    }

    @Override
    public void setFetchSize(int fetchSize) {
        source.setFetchSize(fetchSize);
    }

    @Override
    public int getFetchSize() {
        return source.getFetchSize();
    }

    @Override
    public boolean isLazy() {
        return source.isLazy();
    }

    @Override
    public boolean isClosed() {
        return source.isClosed();
    }

    @Override
    public ResultInterface createShallowCopy(SessionInterface targetSession) {
        ResultInterface copy = source.createShallowCopy(targetSession);
        return copy != null ? new ResultWithPaddedStrings(copy) : null;
    }

}
