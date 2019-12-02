/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.h2.api.ErrorCode;
import org.h2.api.TimestampWithTimeZone;
import org.h2.command.CommandInterface;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.UpdatableRow;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * <p>
 * Represents a result set.
 * </p>
 * <p>
 * Column labels are case-insensitive, quotes are not supported. The first
 * column has the column index 1.
 * </p>
 * <p>
 * Updatable result sets: Result sets are updatable when the result only
 * contains columns from one table, and if it contains all columns of a unique
 * index (primary key or other) of this table. Key columns may not contain NULL
 * (because multiple rows with NULL could exist). In updatable result sets, own
 * changes are visible, but not own inserts and deletes.
 * </p>
 */
public class JdbcResultSet extends TraceObject implements ResultSet, JdbcResultSetBackwardsCompat {

    private final boolean closeStatement;
    private final boolean scrollable;
    private final boolean updatable;
    private ResultInterface result;
    private JdbcConnection conn;
    private JdbcStatement stat;
    private int columnCount;
    private boolean wasNull;
    private Value[] insertRow;
    private Value[] updateRow;
    private HashMap<String, Integer> columnLabelMap;
    private HashMap<Integer, Value[]> patchedRows;
    private JdbcPreparedStatement preparedStatement;
    private final CommandInterface command;

    JdbcResultSet(JdbcConnection conn, JdbcStatement stat, CommandInterface command,
            ResultInterface result, int id, boolean closeStatement,
            boolean scrollable, boolean updatable) {
        setTrace(conn.getSession().getTrace(), TraceObject.RESULT_SET, id);
        this.conn = conn;
        this.stat = stat;
        this.command = command;
        this.result = result;
        this.columnCount = result.getVisibleColumnCount();
        this.closeStatement = closeStatement;
        this.scrollable = scrollable;
        this.updatable = updatable;
    }

    JdbcResultSet(JdbcConnection conn, JdbcPreparedStatement preparedStatement,
            CommandInterface command, ResultInterface result, int id, boolean closeStatement,
            boolean scrollable, boolean updatable,
            HashMap<String, Integer> columnLabelMap) {
        this(conn, preparedStatement, command, result, id, closeStatement, scrollable,
                updatable);
        this.columnLabelMap = columnLabelMap;
        this.preparedStatement = preparedStatement;
    }

    /**
     * Moves the cursor to the next row of the result set.
     *
     * @return true if successful, false if there are no more rows
     */
    @Override
    public boolean next() throws SQLException {
        try {
            debugCodeCall("next");
            checkClosed();
            return nextRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the meta data of this result set.
     *
     * @return the meta data
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        try {
            int id = getNextId(TraceObject.RESULT_SET_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSetMetaData",
                        TraceObject.RESULT_SET_META_DATA, id, "getMetaData()");
            }
            checkClosed();
            String catalog = conn.getCatalog();
            return new JdbcResultSetMetaData(this, null, result, catalog, conn.getSession().getTrace(), id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether the last column accessed was null.
     *
     * @return true if the last column accessed was null
     */
    @Override
    public boolean wasNull() throws SQLException {
        try {
            debugCodeCall("wasNull");
            checkClosed();
            return wasNull;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Searches for a specific column in the result set. A case-insensitive
     * search is made.
     *
     * @param columnLabel the column label
     * @return the column index (1,2,...)
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        try {
            debugCodeCall("findColumn", columnLabel);
            return getColumnIndex(columnLabel);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes the result set.
     */
    @Override
    public void close() throws SQLException {
        try {
            debugCodeCall("close");
            closeInternal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Close the result set. This method also closes the statement if required.
     */
    void closeInternal() throws SQLException {
        if (result != null) {
            try {
                if (result.isLazy()) {
                    stat.onLazyResultSetClose(command, preparedStatement == null);
                }
                result.close();
                if (closeStatement && stat != null) {
                    stat.close();
                }
            } finally {
                columnCount = 0;
                result = null;
                stat = null;
                conn = null;
                insertRow = null;
                updateRow = null;
            }
        }
    }

    /**
     * Returns the statement that created this object.
     *
     * @return the statement or prepared statement, or null if created by a
     *         DatabaseMetaData call.
     */
    @Override
    public Statement getStatement() throws SQLException {
        try {
            debugCodeCall("getStatement");
            checkClosed();
            if (closeStatement) {
                // if the result set was opened by a DatabaseMetaData call
                return null;
            }
            return stat;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the first warning reported by calls on this object.
     *
     * @return null
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            debugCodeCall("getWarnings");
            checkClosed();
            return null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all warnings.
     */
    @Override
    public void clearWarnings() throws SQLException {
        try {
            debugCodeCall("clearWarnings");
            checkClosed();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getString(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getString", columnIndex);
            return get(columnIndex).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getString(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getString", columnLabel);
            return get(columnLabel).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int getInt(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getInt", columnIndex);
            return get(columnIndex).getInt();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int getInt(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getInt", columnLabel);
            return get(columnLabel).getInt();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBigDecimal", columnIndex);
            return get(columnIndex).getBigDecimal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Date getDate(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getDate", columnIndex);
            return get(columnIndex).getDate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Time getTime(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getTime", columnIndex);
            return get(columnIndex).getTime();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getTimestamp", columnIndex);
            return get(columnIndex).getTimestamp();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBigDecimal", columnLabel);
            return get(columnLabel).getBigDecimal();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Date getDate(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getDate", columnLabel);
            return get(columnLabel).getDate();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Time getTime(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getTime", columnLabel);
            return get(columnLabel).getTime();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getTimestamp", columnLabel);
            return get(columnLabel).getTimestamp();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param columnIndex (1,2,...)
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getObject", columnIndex);
            Value v = get(columnIndex);
            return conn.convertToDefaultObject(v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param columnLabel the column label
     * @return the value or null
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getObject", columnLabel);
            Value v = get(columnLabel);
            return conn.convertToDefaultObject(v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnIndex);
            return get(columnIndex).getBoolean();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBoolean", columnLabel);
            return get(columnLabel).getBoolean();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getByte", columnIndex);
            return get(columnIndex).getByte();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte getByte(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getByte", columnLabel);
            return get(columnLabel).getByte();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public short getShort(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getShort", columnIndex);
            return get(columnIndex).getShort();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public short getShort(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getShort", columnLabel);
            return get(columnLabel).getShort();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public long getLong(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getLong", columnIndex);
            return get(columnIndex).getLong();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public long getLong(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getLong", columnLabel);
            return get(columnLabel).getLong();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public float getFloat(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getFloat", columnIndex);
            return get(columnIndex).getFloat();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public float getFloat(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getFloat", columnLabel);
            return get(columnLabel).getFloat();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public double getDouble(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getDouble", columnIndex);
            return get(columnIndex).getDouble();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public double getDouble(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getDouble", columnLabel);
            return get(columnLabel).getDouble();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @deprecated use {@link #getBigDecimal(String)}
     *
     * @param columnLabel the column label
     * @param scale the scale of the returned value
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" +
                        StringUtils.quoteJavaString(columnLabel)+", "+scale+");");
            }
            if (scale < 0) {
                throw DbException.getInvalidValueException("scale", scale);
            }
            BigDecimal bd = get(columnLabel).getBigDecimal();
            return bd == null ? null : ValueDecimal.setScale(bd, scale);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a BigDecimal.
     *
     * @deprecated use {@link #getBigDecimal(int)}
     *
     * @param columnIndex (1,2,...)
     * @param scale the scale of the returned value
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" + columnIndex + ", " + scale + ");");
            }
            if (scale < 0) {
                throw DbException.getInvalidValueException("scale", scale);
            }
            BigDecimal bd = get(columnIndex).getBigDecimal();
            return bd == null ? null : ValueDecimal.setScale(bd, scale);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * @deprecated since JDBC 2.0, use getCharacterStream
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw unsupported("unicodeStream");
    }

    /**
     * [Not supported]
     * @deprecated since JDBC 2.0, use setCharacterStream
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw unsupported("unicodeStream");
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException {
        throw unsupported("map");
    }

    /**
     * [Not supported] Gets a column as a object using the specified type
     * mapping.
     */
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException {
        throw unsupported("map");
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" + columnIndex + ", calendar)");
            }
            return DateTimeUtils.convertDate(get(columnIndex), calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a
     * specified time zone.
     *
     * @param columnLabel the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Date getDate(String columnLabel, Calendar calendar)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" +
                        StringUtils.quoteJavaString(columnLabel) +
                        ", calendar)");
            }
            return DateTimeUtils.convertDate(get(columnLabel), calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" + columnIndex + ", calendar)");
            }
            return DateTimeUtils.convertTime(get(columnIndex), calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a
     * specified time zone.
     *
     * @param columnLabel the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Time getTime(String columnLabel, Calendar calendar)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" +
                        StringUtils.quoteJavaString(columnLabel) +
                        ", calendar)");
            }
            return DateTimeUtils.convertTime(get(columnLabel), calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp using a
     * specified time zone.
     *
     * @param columnIndex (1,2,...)
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar calendar)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" + columnIndex + ", calendar)");
            }
            Value value = get(columnIndex);
            return DateTimeUtils.convertTimestamp(value, calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     *
     * @param columnLabel the column label
     * @param calendar the calendar
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar calendar)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" +
                        StringUtils.quoteJavaString(columnLabel) +
                        ", calendar)");
            }
            Value value = get(columnLabel);
            return DateTimeUtils.convertTimestamp(value, calendar);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Blob", TraceObject.BLOB,
                                id, "getBlob(" + columnIndex + ")");
            }
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.BLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Blob", TraceObject.BLOB,
                                id, "getBlob(" + quote(columnLabel) + ")");
            }
            Value v = get(columnLabel);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBytes", columnIndex);
            return get(columnIndex).getBytes();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBytes", columnLabel);
            return get(columnLabel).getBytes();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnIndex);
            return get(columnIndex).getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getBinaryStream", columnLabel);
            return get(columnLabel).getInputStream();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }


    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" + columnIndex + ")");
            }
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" +
                                quote(columnLabel) + ")");
            }
            Value v = get(columnLabel);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an Array.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Array getArray(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.ARRAY);
            if (isDebugEnabled()) {
                debugCodeAssign("Array", TraceObject.ARRAY, id, "getArray(" + columnIndex + ")");
            }
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcArray(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an Array.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.ARRAY);
            if (isDebugEnabled()) {
                debugCodeAssign("Array", TraceObject.ARRAY, id, "getArray(" +
                                quote(columnLabel) + ")");
            }
            Value v = get(columnLabel);
            return v == ValueNull.INSTANCE ? null : new JdbcArray(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnIndex);
            String s = get(columnIndex).getString();
            return s == null ? null : IOUtils.getInputStreamFromString(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an input stream.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getAsciiStream", columnLabel);
            String s = get(columnLabel).getString();
            return IOUtils.getInputStreamFromString(s);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getCharacterStream", columnLabel);
            return get(columnLabel).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw unsupported("url");
    }

    /**
     * [Not supported]
     */
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw unsupported("url");
    }

    // =============================================================

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        try {
            debugCodeCall("updateNull", columnIndex);
            update(columnIndex, ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        try {
            debugCodeCall("updateNull", columnLabel);
            update(columnLabel, ValueNull.INSTANCE);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if result set is closed or not updatable
     */
    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean("+quote(columnLabel)+", "+x+");");
            }
            update(columnLabel, ValueBoolean.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateByte("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueByte.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateByte("+columnLabel+", "+x+");");
            }
            update(columnLabel, ValueByte.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes("+quote(columnLabel)+", x);");
            }
            update(columnLabel, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateShort("+columnIndex+", (short) "+x+");");
            }
            update(columnIndex, ValueShort.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateShort("+quote(columnLabel)+", (short) "+x+");");
            }
            update(columnLabel, ValueShort.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateInt("+columnIndex+", "+x+");");
            }
            update(columnIndex, ValueInt.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateInt("+quote(columnLabel)+", "+x+");");
            }
            update(columnLabel, ValueInt.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateLong("+columnIndex+", "+x+"L);");
            }
            update(columnIndex, ValueLong.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateLong("+quote(columnLabel)+", "+x+"L);");
            }
            update(columnLabel, ValueLong.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat("+columnIndex+", "+x+"f);");
            }
            update(columnIndex, ValueFloat.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat("+quote(columnLabel)+", "+x+"f);");
            }
            update(columnLabel, ValueFloat.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble("+columnIndex+", "+x+"d);");
            }
            update(columnIndex, ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble("+quote(columnLabel)+", "+x+"d);");
            }
            update(columnLabel, ValueDouble.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal("+columnIndex+", " + quoteBigDecimal(x) + ");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE
                    : ValueDecimal.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal(" + quote(columnLabel) + ", " +
                        quoteBigDecimal(x) + ");");
            }
            update(columnLabel, x == null ? (Value) ValueNull.INSTANCE
                    : ValueDecimal.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateString("+columnIndex+", "+quote(x)+");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE
                    : ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateString("+quote(columnLabel)+", "+quote(x)+");");
            }
            update(columnLabel, x == null ? (Value) ValueNull.INSTANCE
                    : ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDate("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateDate("+quote(columnLabel)+", x);");
            }
            update(columnLabel, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTime("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTime("+quote(columnLabel)+", x);");
            }
            update(columnLabel, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp("+columnIndex+", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE
                    : ValueTimestamp.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp("+quote(columnLabel)+", x);");
            }
            update(columnLabel, x == null ? (Value) ValueNull.INSTANCE
                    : ValueTimestamp.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        updateAsciiStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        updateAsciiStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        updateAsciiStream(columnLabel, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        updateAsciiStream(columnLabel, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream("+quote(columnLabel)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        updateBinaryStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        updateBinaryStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        updateBinaryStream(columnLabel, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException {
        updateBinaryStream(columnLabel, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBinaryStream(String columnLabel, InputStream x,
            long length) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream("+quote(columnLabel)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException {
        updateCharacterStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        updateCharacterStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader x, int length)
            throws SQLException {
        updateCharacterStream(columnLabel, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader x)
            throws SQLException {
        updateCharacterStream(columnLabel, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateCharacterStream(String columnLabel, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream("+quote(columnLabel)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param scale is ignored
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+columnIndex+", x, "+scale+");");
            }
            update(columnIndex, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }



    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param scale is ignored
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(String columnLabel, Object x, int scale)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+quote(columnLabel)+", x, "+scale+");");
            }
            update(columnLabel, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+columnIndex+", x);");
            }
            update(columnIndex, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateObject("+quote(columnLabel)+", x);");
            }
            update(columnLabel, convertToUnknownValue(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw unsupported("ref");
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(int columnIndex, InputStream x) throws SQLException {
        updateBlob(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(int columnIndex, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+columnIndex+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+columnIndex+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+quote(columnLabel)+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(String columnLabel, InputStream x) throws SQLException {
        updateBlob(columnLabel, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateBlob(String columnLabel, InputStream x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob("+quote(columnLabel)+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createBlob(x, -1);
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+columnIndex+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(int columnIndex, Reader x) throws SQLException {
        updateClob(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(int columnIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+columnIndex+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+quote(columnLabel)+", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(String columnLabel, Reader x) throws SQLException {
        updateClob(columnLabel, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateClob(String columnLabel, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateClob("+quote(columnLabel)+", x, " + length + "L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw unsupported("setArray");
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw unsupported("setArray");
    }

    /**
     * [Not supported] Gets the cursor name if it was defined. This feature is
     * superseded by updateX methods. This method throws a SQLException because
     * cursor names are not supported.
     */
    @Override
    public String getCursorName() throws SQLException {
        throw unsupported("cursorName");
    }

    /**
     * Gets the current row number. The first row is row 1, the second 2 and so
     * on. This method returns 0 before the first and after the last row.
     *
     * @return the row number
     */
    @Override
    public int getRow() throws SQLException {
        try {
            debugCodeCall("getRow");
            checkClosed();
            if (result.isAfterLast()) {
                return 0;
            }
            int rowId = result.getRowId();
            return rowId + 1;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set concurrency. Result sets are only updatable if the
     * statement was created with updatable concurrency, and if the result set
     * contains all columns of the primary key or of a unique index of a table.
     *
     * @return ResultSet.CONCUR_UPDATABLE if the result set is updatable, or
     *         ResultSet.CONCUR_READ_ONLY otherwise
     */
    @Override
    public int getConcurrency() throws SQLException {
        try {
            debugCodeCall("getConcurrency");
            checkClosed();
            if (!updatable) {
                return ResultSet.CONCUR_READ_ONLY;
            }
            UpdatableRow row = new UpdatableRow(conn, result);
            return row.isUpdatable() ? ResultSet.CONCUR_UPDATABLE
                    : ResultSet.CONCUR_READ_ONLY;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the fetch direction.
     *
     * @return the direction: FETCH_FORWARD
     */
    @Override
    public int getFetchDirection() throws SQLException {
        try {
            debugCodeCall("getFetchDirection");
            checkClosed();
            return ResultSet.FETCH_FORWARD;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the number of rows suggested to read in one step.
     *
     * @return the current fetch size
     */
    @Override
    public int getFetchSize() throws SQLException {
        try {
            debugCodeCall("getFetchSize");
            checkClosed();
            return result.getFetchSize();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the number of rows suggested to read in one step. This value cannot
     * be higher than the maximum rows (setMaxRows) set by the statement or
     * prepared statement, otherwise an exception is throws. Setting the value
     * to 0 will set the default value. The default value can be changed using
     * the system property h2.serverResultSetFetchSize.
     *
     * @param rows the number of rows
     */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();
            if (rows < 0) {
                throw DbException.getInvalidValueException("rows", rows);
            } else if (rows > 0) {
                if (stat != null) {
                    int maxRows = stat.getMaxRows();
                    if (maxRows > 0 && rows > maxRows) {
                        throw DbException.getInvalidValueException("rows", rows);
                    }
                }
            } else {
                rows = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
            }
            result.setFetchSize(rows);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * Sets (changes) the fetch direction for this result set. This method
     * should only be called for scrollable result sets, otherwise it will throw
     * an exception (no matter what direction is used).
     *
     * @param direction the new fetch direction
     * @throws SQLException Unsupported Feature if the method is called for a
     *             forward-only result set
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        debugCodeCall("setFetchDirection", direction);
        // ignore FETCH_FORWARD, that's the default value, which we do support
        if (direction != ResultSet.FETCH_FORWARD) {
            throw unsupported("setFetchDirection");
        }
    }

    /**
     * Get the result set type.
     *
     * @return the result set type (TYPE_FORWARD_ONLY, TYPE_SCROLL_INSENSITIVE
     *         or TYPE_SCROLL_SENSITIVE)
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public int getType() throws SQLException {
        try {
            debugCodeCall("getType");
            checkClosed();
            return stat == null ? ResultSet.TYPE_FORWARD_ONLY : stat.resultSetType;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is before the first row, that means next()
     * was not called yet, and there is at least one row.
     *
     * @return if there are results and the current position is before the first
     *         row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isBeforeFirst() throws SQLException {
        try {
            debugCodeCall("isBeforeFirst");
            checkClosed();
            return result.getRowId() < 0 && result.hasNext();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is after the last row, that means next()
     * was called and returned false, and there was at least one row.
     *
     * @return if there are results and the current position is after the last
     *         row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isAfterLast() throws SQLException {
        try {
            debugCodeCall("isAfterLast");
            checkClosed();
            return result.getRowId() > 0 && result.isAfterLast();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is row 1, that means next() was called
     * once and returned true.
     *
     * @return if the current position is the first row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isFirst() throws SQLException {
        try {
            debugCodeCall("isFirst");
            checkClosed();
            return result.getRowId() == 0 && !result.isAfterLast();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is the last row, that means next() was
     * called and did not yet returned false, but will in the next call.
     *
     * @return if the current position is the last row
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean isLast() throws SQLException {
        try {
            debugCodeCall("isLast");
            checkClosed();
            int rowId = result.getRowId();
            return rowId >= 0 && !result.isAfterLast() && !result.hasNext();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to before the first row, that means resets the
     * result set.
     *
     * @throws SQLException if the result set is closed
     */
    @Override
    public void beforeFirst() throws SQLException {
        try {
            debugCodeCall("beforeFirst");
            checkClosed();
            if (result.getRowId() >= 0) {
                resetResult();
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to after the last row, that means after the
     * end.
     *
     * @throws SQLException if the result set is closed
     */
    @Override
    public void afterLast() throws SQLException {
        try {
            debugCodeCall("afterLast");
            checkClosed();
            while (nextRow()) {
                // nothing
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
}

    /**
     * Moves the current position to the first row. This is the same as calling
     * beforeFirst() followed by next().
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean first() throws SQLException {
        try {
            debugCodeCall("first");
            checkClosed();
            if (result.getRowId() >= 0) {
                resetResult();
            }
            return nextRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the last row.
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean last() throws SQLException {
        try {
            debugCodeCall("last");
            checkClosed();
            if (result.isAfterLast()) {
                resetResult();
            }
            while (result.hasNext()) {
                nextRow();
            }
            return isOnValidRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row.
     *
     * @param rowNumber the row number. 0 is not allowed, 1 means the first row,
     *            2 the second. -1 means the last row, -2 the row before the
     *            last row. If the value is too large, the position is moved
     *            after the last row, if if the value is too small it is moved
     *            before the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean absolute(int rowNumber) throws SQLException {
        try {
            debugCodeCall("absolute", rowNumber);
            checkClosed();
            if (rowNumber < 0) {
                rowNumber = result.getRowCount() + rowNumber + 1;
            }
            if (--rowNumber < result.getRowId()) {
                resetResult();
            }
            while (result.getRowId() < rowNumber) {
                if (!nextRow()) {
                    return false;
                }
            }
            return isOnValidRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row relative to the current row.
     *
     * @param rowCount 0 means don't do anything, 1 is the next row, -1 the
     *            previous. If the value is too large, the position is moved
     *            after the last row, if if the value is too small it is moved
     *            before the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean relative(int rowCount) throws SQLException {
        try {
            debugCodeCall("relative", rowCount);
            checkClosed();
            if (rowCount < 0) {
                rowCount = result.getRowId() + rowCount + 1;
                resetResult();
            }
            for (int i = 0; i < rowCount; i++) {
                if (!nextRow()) {
                    return false;
                }
            }
            return isOnValidRow();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the cursor to the last row, or row before first row if the current
     * position is the first row.
     *
     * @return true if there is a row available, false if not
     * @throws SQLException if the result set is closed
     */
    @Override
    public boolean previous() throws SQLException {
        try {
            debugCodeCall("previous");
            checkClosed();
            return relative(-1);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the insert row. The current row is
     * remembered.
     *
     * @throws SQLException if the result set is closed or is not updatable
     */
    @Override
    public void moveToInsertRow() throws SQLException {
        try {
            debugCodeCall("moveToInsertRow");
            checkUpdatable();
            insertRow = new Value[columnCount];
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the current row.
     *
     * @throws SQLException if the result set is closed or is not updatable
     */
    @Override
    public void moveToCurrentRow() throws SQLException {
        try {
            debugCodeCall("moveToCurrentRow");
            checkUpdatable();
            insertRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was updated (by somebody else or the caller).
     *
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowUpdated() throws SQLException {
        try {
            debugCodeCall("rowUpdated");
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was inserted.
     *
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowInserted() throws SQLException {
        try {
            debugCodeCall("rowInserted");
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was deleted (by somebody else or the caller).
     *
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowDeleted() throws SQLException {
        try {
            debugCodeCall("rowDeleted");
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Inserts the current row. The current position must be the insert row.
     *
     * @throws SQLException if the result set is closed or if not on the insert
     *             row, or if the result set it not updatable
     */
    @Override
    public void insertRow() throws SQLException {
        try {
            debugCodeCall("insertRow");
            checkUpdatable();
            if (insertRow == null) {
                throw DbException.get(ErrorCode.NOT_ON_UPDATABLE_ROW);
            }
            getUpdatableRow().insertRow(insertRow);
            insertRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates the current row.
     *
     * @throws SQLException if the result set is closed, if the current row is
     *             the insert row or if not on a valid row, or if the result set
     *             it not updatable
     */
    @Override
    public void updateRow() throws SQLException {
        try {
            debugCodeCall("updateRow");
            checkUpdatable();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NOT_ON_UPDATABLE_ROW);
            }
            checkOnValidRow();
            if (updateRow != null) {
                UpdatableRow row = getUpdatableRow();
                Value[] current = new Value[columnCount];
                for (int i = 0; i < updateRow.length; i++) {
                    current[i] = get(i + 1);
                }
                row.updateRow(current, updateRow);
                for (int i = 0; i < updateRow.length; i++) {
                    if (updateRow[i] == null) {
                        updateRow[i] = current[i];
                    }
                }
                Value[] patch = row.readRow(updateRow);
                patchCurrentRow(patch);
                updateRow = null;
            }
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Deletes the current row.
     *
     * @throws SQLException if the result set is closed, if the current row is
     *             the insert row or if not on a valid row, or if the result set
     *             it not updatable
     */
    @Override
    public void deleteRow() throws SQLException {
        try {
            debugCodeCall("deleteRow");
            checkUpdatable();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NOT_ON_UPDATABLE_ROW);
            }
            checkOnValidRow();
            getUpdatableRow().deleteRow(result.currentRow());
            updateRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Re-reads the current row from the database.
     *
     * @throws SQLException if the result set is closed or if the current row is
     *             the insert row or if the row has been deleted or if not on a
     *             valid row
     */
    @Override
    public void refreshRow() throws SQLException {
        try {
            debugCodeCall("refreshRow");
            checkClosed();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
            }
            checkOnValidRow();
            patchCurrentRow(getUpdatableRow().readRow(result.currentRow()));
            updateRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Cancels updating a row.
     *
     * @throws SQLException if the result set is closed or if the current row is
     *             the insert row
     */
    @Override
    public void cancelRowUpdates() throws SQLException {
        try {
            debugCodeCall("cancelRowUpdates");
            checkClosed();
            if (insertRow != null) {
                throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
            }
            updateRow = null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    private UpdatableRow getUpdatableRow() throws SQLException {
        UpdatableRow row = new UpdatableRow(conn, result);
        if (!row.isUpdatable()) {
            throw DbException.get(ErrorCode.RESULT_SET_NOT_UPDATABLE);
        }
        return row;
    }

    private int getColumnIndex(String columnLabel) {
        checkClosed();
        if (columnLabel == null) {
            throw DbException.getInvalidValueException("columnLabel", null);
        }
        if (columnCount >= 3) {
            // use a hash table if more than 2 columns
            if (columnLabelMap == null) {
                HashMap<String, Integer> map = new HashMap<>(columnCount);
                // column labels have higher priority
                for (int i = 0; i < columnCount; i++) {
                    String c = StringUtils.toUpperEnglish(result.getAlias(i));
                    mapColumn(map, c, i);
                }
                for (int i = 0; i < columnCount; i++) {
                    String colName = result.getColumnName(i);
                    if (colName != null) {
                        colName = StringUtils.toUpperEnglish(colName);
                        mapColumn(map, colName, i);
                        String tabName = result.getTableName(i);
                        if (tabName != null) {
                            colName = StringUtils.toUpperEnglish(tabName) + "." + colName;
                            mapColumn(map, colName, i);
                        }
                    }
                }
                // assign at the end so concurrent access is supported
                columnLabelMap = map;
                if (preparedStatement != null) {
                    preparedStatement.setCachedColumnLabelMap(columnLabelMap);
                }
            }
            Integer index = columnLabelMap.get(StringUtils.toUpperEnglish(columnLabel));
            if (index == null) {
                throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnLabel);
            }
            return index.intValue() + 1;
        }
        for (int i = 0; i < columnCount; i++) {
            if (columnLabel.equalsIgnoreCase(result.getAlias(i))) {
                return i + 1;
            }
        }
        int idx = columnLabel.indexOf('.');
        if (idx > 0) {
            String table = columnLabel.substring(0, idx);
            String col = columnLabel.substring(idx+1);
            for (int i = 0; i < columnCount; i++) {
                if (table.equalsIgnoreCase(result.getTableName(i)) &&
                        col.equalsIgnoreCase(result.getColumnName(i))) {
                    return i + 1;
                }
            }
        } else {
            for (int i = 0; i < columnCount; i++) {
                if (columnLabel.equalsIgnoreCase(result.getColumnName(i))) {
                    return i + 1;
                }
            }
        }
        throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnLabel);
    }

    private static void mapColumn(HashMap<String, Integer> map, String label,
            int index) {
        // put the index (usually that's the only operation)
        Integer old = map.put(label, index);
        if (old != null) {
            // if there was a clash (which is seldom),
            // put the old one back
            map.put(label, old);
        }
    }

    private void checkColumnIndex(int columnIndex) {
        checkClosed();
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw DbException.getInvalidValueException("columnIndex", columnIndex);
        }
    }

    /**
     * Check if this result set is closed.
     *
     * @throws DbException if it is closed
     */
    void checkClosed() {
        if (result == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        if (stat != null) {
            stat.checkClosed();
        }
        if (conn != null) {
            conn.checkClosed();
        }
    }

    private boolean isOnValidRow() {
        return result.getRowId() >= 0 && !result.isAfterLast();
    }

    private void checkOnValidRow() {
        if (!isOnValidRow()) {
            throw DbException.get(ErrorCode.NO_DATA_AVAILABLE);
        }
    }

    /**
     * INTERNAL
     *
     * @param columnIndex
     *            index of a column
     * @return internal representation of the value in the specified column
     */
    public Value get(int columnIndex) {
        checkColumnIndex(columnIndex);
        checkOnValidRow();
        Value[] list;
        if (patchedRows == null) {
            list = result.currentRow();
        } else {
            list = patchedRows.get(result.getRowId());
            if (list == null) {
                list = result.currentRow();
            }
        }
        Value value = list[columnIndex - 1];
        wasNull = value == ValueNull.INSTANCE;
        return value;
    }

    private Value get(String columnLabel) {
        int columnIndex = getColumnIndex(columnLabel);
        return get(columnIndex);
    }

    private void update(String columnLabel, Value v) {
        int columnIndex = getColumnIndex(columnLabel);
        update(columnIndex, v);
    }

    private void update(int columnIndex, Value v) {
        checkUpdatable();
        checkColumnIndex(columnIndex);
        if (insertRow != null) {
            insertRow[columnIndex - 1] = v;
        } else {
            if (updateRow == null) {
                updateRow = new Value[columnCount];
            }
            updateRow[columnIndex - 1] = v;
        }
    }

    private boolean nextRow() {
        if (result.isLazy() && stat.isCancelled()) {
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
        boolean next = result.next();
        if (!next && !scrollable) {
            result.close();
        }
        return next;
    }

    private void resetResult() {
        if (!scrollable) {
            throw DbException.get(ErrorCode.RESULT_SET_NOT_SCROLLABLE);
        }
        result.reset();
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param columnIndex (1,2,...)
     */
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     *
     * @param columnLabel the column label
     */
    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     */
    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     */
    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw unsupported("rowId");
    }

    /**
     * Returns the current result set holdability.
     *
     * @return the holdability
     * @throws SQLException if the connection is closed
     */
    @Override
    public int getHoldability() throws SQLException {
        try {
            debugCodeCall("getHoldability");
            checkClosed();
            return conn.getHoldability();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether this result set is closed.
     *
     * @return true if the result set is closed
     */
    @Override
    public boolean isClosed() throws SQLException {
        try {
            debugCodeCall("isClosed");
            return result == null;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNString(int columnIndex, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNString("+columnIndex+", "+quote(x)+");");
            }
            update(columnIndex, x == null ? (Value)
                    ValueNull.INSTANCE : ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNString(String columnLabel, String x) throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNString("+quote(columnLabel)+", "+quote(x)+");");
            }
            update(columnLabel, x == null ? (Value) ValueNull.INSTANCE :
                    ValueString.get(x));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateNClob(int columnIndex, NClob x) throws SQLException {
        throw unsupported("NClob");
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(int columnIndex, Reader x) throws SQLException {
        updateClob(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(int columnIndex, Reader x, long length)
            throws SQLException {
        updateClob(columnIndex, x, length);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(String columnLabel, Reader x) throws SQLException {
        updateClob(columnLabel, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the length
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNClob(String columnLabel, Reader x, long length)
            throws SQLException {
        updateClob(columnLabel, x, length);
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateNClob(String columnLabel, NClob x) throws SQLException {
        throw unsupported("NClob");
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + columnIndex + ")");
            }
            Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        try {
            int id = getNextId(TraceObject.CLOB);
            if (isDebugEnabled()) {
                debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + columnLabel + ")");
            }
            Value v = get(columnLabel);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML
     * object.
     */
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw unsupported("SQLXML");
    }

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML
     * object.
     */
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw unsupported("SQLXML");
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     */
    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException {
        throw unsupported("SQLXML");
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     */
    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException {
        throw unsupported("SQLXML");
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getNString(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNString", columnIndex);
            return get(columnIndex).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public String getNString(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getNString", columnLabel);
            return get(columnLabel).getString();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnIndex (1,2,...)
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a reader.
     *
     * @param columnLabel the column label
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        try {
            debugCodeCall("getNCharacterStream", columnLabel);
            return get(columnLabel).getReader();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        updateNCharacterStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnIndex (1,2,...)
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream("+columnIndex+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnIndex, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader x)
            throws SQLException {
        updateNCharacterStream(columnLabel, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     *
     * @param columnLabel the column label
     * @param x the value
     * @param length the number of characters
     * @throws SQLException if the result set is closed or not updatable
     */
    @Override
    public void updateNCharacterStream(String columnLabel, Reader x, long length)
            throws SQLException {
        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream("+quote(columnLabel)+", x, "+length+"L);");
            }
            checkClosed();
            Value v = conn.createClob(x, length);
            update(columnLabel, v);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param columnIndex the column index (1, 2, ...)
     * @param type the class of the returned value
     * @return the value
     * @throws SQLException if the column is not found or if the result set is
     *             closed
     */
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        try {
            if (type == null) {
                throw DbException.getInvalidValueException("type", type);
            }
            debugCodeCall("getObject", columnIndex);
            Value value = get(columnIndex);
            return extractObjectOfType(type, value);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. The data is
     * de-serialized into a Java object (on the client side).
     *
     * @param columnName the column name
     * @param type the class of the returned value
     * @return the value
     */
    @Override
    public <T> T getObject(String columnName, Class<T> type) throws SQLException {
        try {
            if (type == null) {
                throw DbException.getInvalidValueException("type", type);
            }
            debugCodeCall("getObject", columnName);
            Value value = get(columnName);
            return extractObjectOfType(type, value);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private <T> T extractObjectOfType(Class<T> type, Value value) throws SQLException {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        if (type == BigDecimal.class) {
            return type.cast(value.getBigDecimal());
        } else if (type == BigInteger.class) {
            return type.cast(value.getBigDecimal().toBigInteger());
        } else if (type == String.class) {
            return type.cast(value.getString());
        } else if (type == Boolean.class) {
            return type.cast(value.getBoolean());
        } else if (type == Byte.class) {
            return type.cast(value.getByte());
        } else if (type == Short.class) {
            return type.cast(value.getShort());
        } else if (type == Integer.class) {
            return type.cast(value.getInt());
        } else if (type == Long.class) {
            return type.cast(value.getLong());
        } else if (type == Float.class) {
            return type.cast(value.getFloat());
        } else if (type == Double.class) {
            return type.cast(value.getDouble());
        } else if (type == Date.class) {
            return type.cast(value.getDate());
        } else if (type == Time.class) {
            return type.cast(value.getTime());
        } else if (type == Timestamp.class) {
            return type.cast(value.getTimestamp());
        } else if (type == java.util.Date.class) {
            return type.cast(new java.util.Date(value.getTimestamp().getTime()));
        } else if (type == Calendar.class) {
            Calendar calendar = DateTimeUtils.createGregorianCalendar();
            calendar.setTime(value.getTimestamp());
            return type.cast(calendar);
        } else if (type == UUID.class) {
            return type.cast(value.getObject());
        } else if (type == byte[].class) {
            return type.cast(value.getBytes());
        } else if (type == java.sql.Array.class) {
            int id = getNextId(TraceObject.ARRAY);
            return type.cast(value == ValueNull.INSTANCE ? null : new JdbcArray(conn, value, id));
        } else if (type == Blob.class) {
            int id = getNextId(TraceObject.BLOB);
            return type.cast(value == ValueNull.INSTANCE ? null : new JdbcBlob(conn, value, id));
        } else if (type == Clob.class) {
            int id = getNextId(TraceObject.CLOB);
            return type.cast(value == ValueNull.INSTANCE ? null : new JdbcClob(conn, value, id));
        } else if (type == TimestampWithTimeZone.class) {
            return type.cast(value.getObject());
        } else if (DataType.isGeometryClass(type)) {
            return type.cast(value.getObject());
        } else if (type == LocalDateTimeUtils.LOCAL_DATE) {
            return type.cast(LocalDateTimeUtils.valueToLocalDate(value));
        } else if (type == LocalDateTimeUtils.LOCAL_TIME) {
            return type.cast(LocalDateTimeUtils.valueToLocalTime(value));
        } else if (type == LocalDateTimeUtils.LOCAL_DATE_TIME) {
            return type.cast(LocalDateTimeUtils.valueToLocalDateTime(value));
        } else if (type == LocalDateTimeUtils.INSTANT) {
            return type.cast(LocalDateTimeUtils.valueToInstant(value));
        } else if (type == LocalDateTimeUtils.OFFSET_DATE_TIME) {
            return type.cast(LocalDateTimeUtils.valueToOffsetDateTime(value));
        } else {
            throw unsupported(type.getName());
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + result;
    }

    private void patchCurrentRow(Value[] row) {
        boolean changed = false;
        Value[] current = result.currentRow();
        CompareMode mode = conn.getCompareMode();
        for (int i = 0; i < row.length; i++) {
            if (row[i].compareTo(current[i], mode) != 0) {
                changed = true;
                break;
            }
        }
        if (patchedRows == null) {
            patchedRows = new HashMap<>();
        }
        Integer rowId = result.getRowId();
        if (!changed) {
            patchedRows.remove(rowId);
        } else {
            patchedRows.put(rowId, row);
        }
    }

    private Value convertToUnknownValue(Object x) {
        checkClosed();
        return DataType.convertToValue(conn.getSession(), x, Value.UNKNOWN);
    }

    private void checkUpdatable() {
        checkClosed();
        if (!updatable) {
            throw DbException.get(ErrorCode.RESULT_SET_READONLY);
        }
    }

}
