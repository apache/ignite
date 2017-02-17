/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.jdbc2;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC result set implementation.
 */
public class JdbcResultSet implements ResultSet {
    /** Uuid. */
    private final UUID uuid;

    /** Statement. */
    private final JdbcStatement stmt;

    /** Table names. */
    private final List<String> tbls;

    /** Column names. */
    private final List<String> cols;

    /** Class names. */
    private final List<String> types;

    /** Rows cursor iterator. */
    private Iterator<List<?>> it;

    /** Finished flag. */
    private boolean finished;

    /** Current position. */
    private int pos;

    /** Current. */
    private List<Object> curr;

    /** Closed flag. */
    private boolean closed;

    /** Was {@code NULL} flag. */
    private boolean wasNull;

    /** Fetch size. */
    private int fetchSize;

    /** Which query task to use under the hood - {@link JdbcQueryTaskV2} if {@code true}, {@link JdbcQueryTask} otherwise. */
    private final boolean useNewQryTask;

    /**
     * Creates new result set.
     *
     * @param uuid Query UUID.
     * @param stmt Statement.
     * @param tbls Table names.
     * @param cols Column names.
     * @param types Types.
     * @param fields Fields.
     */
    JdbcResultSet(@Nullable UUID uuid, JdbcStatement stmt, List<String> tbls, List<String> cols,
        List<String> types, Collection<List<?>> fields, boolean finished) {
        this(uuid, stmt, tbls, cols, types, fields, finished, false);
    }

    /**
     * Creates new result set that will be based on {@link JdbcQueryTaskV2}. This method is intended for use inside
     *     {@link JdbcStatement} only.
     *
     * @param uuid Query UUID.
     * @param stmt Statement.
     * @param tbls Table names.
     * @param cols Column names.
     * @param types Types.
     * @param fields Fields.
     */
    static JdbcResultSet resultSetForQueryTaskV2(@Nullable UUID uuid, JdbcStatement stmt, List<String> tbls,
            List<String> cols, List<String> types, Collection<List<?>> fields, boolean finished) {
        return new JdbcResultSet(uuid, stmt, tbls, cols, types, fields, finished, true);
    }

    /**
     * Creates new result set.
     *
     * @param uuid Query UUID.
     * @param stmt Statement.
     * @param tbls Table names.
     * @param cols Column names.
     * @param types Types.
     * @param fields Fields.
     * @param useNewQryTask Which query task to use under the hood - {@link JdbcQueryTaskV2} if {@code true},
     *     {@link JdbcQueryTask} otherwise.
     */
    private JdbcResultSet(@Nullable UUID uuid, JdbcStatement stmt, List<String> tbls, List<String> cols,
        List<String> types, Collection<List<?>> fields, boolean finished, boolean useNewQryTask) {
        assert stmt != null;
        assert tbls != null;
        assert cols != null;
        assert types != null;
        assert fields != null;

        this.uuid = uuid;
        this.stmt = stmt;
        this.tbls = tbls;
        this.cols = cols;
        this.types = types;
        this.finished = finished;

        this.it = fields.iterator();

        this.useNewQryTask = useNewQryTask;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean next() throws SQLException {
        ensureNotClosed();

        if (it == null || (stmt.getMaxRows() > 0 && pos >= stmt.getMaxRows())) {
            curr = null;

            return false;
        }
        else if (it.hasNext()) {
            curr = new ArrayList<>(it.next());

            pos++;

            if (finished && !it.hasNext())
                it = null;

            return true;
        }
        else if (!finished) {
            JdbcConnection conn = (JdbcConnection)stmt.getConnection();

            Ignite ignite = conn.ignite();

            UUID nodeId = conn.nodeId();

            boolean loc = nodeId == null;

            if (useNewQryTask) {
                // Connections from new clients send queries with new tasks, so we have to continue in the same manner
                JdbcQueryTaskV2 qryTask = new JdbcQueryTaskV2(loc ? ignite : null, conn.cacheName(), null, true, loc, null,
                    fetchSize, uuid, conn.isLocalQuery(), conn.isCollocatedQuery(), conn.isDistributedJoins());

                try {
                    JdbcQueryTaskV2.QueryResult res =
                        loc ? qryTask.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(qryTask);

                    finished = res.isFinished();

                    it = res.getRows().iterator();

                    return next();
                }
                catch (IgniteSQLException e) {
                    throw e.toJdbcException();
                }
                catch (Exception e) {
                    throw new SQLException("Failed to query Ignite.", e);
                }
            }

            JdbcQueryTask qryTask = new JdbcQueryTask(loc ? ignite : null, conn.cacheName(), null, loc, null,
                fetchSize, uuid, conn.isLocalQuery(), conn.isCollocatedQuery(), conn.isDistributedJoins());

            try {
                JdbcQueryTask.QueryResult res =
                    loc ? qryTask.call() : ignite.compute(ignite.cluster().forNodeId(nodeId)).call(qryTask);

                finished = res.isFinished();

                it = res.getRows().iterator();

                return next();
            }
            catch (IgniteSQLException e) {
                throw e.toJdbcException();
            }
            catch (Exception e) {
                throw new SQLException("Failed to query Ignite.", e);
            }
        }

        it = null;

        return false;
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        if (uuid != null)
            stmt.resSets.remove(this);

        closeInternal();
    }

    /**
     * Marks result set as closed.
     * If this result set is associated with locally executed query then query cursor will also closed.
     */
    void closeInternal() throws SQLException  {
        if (((JdbcConnection)stmt.getConnection()).nodeId() == null && uuid != null) {
            JdbcQueryTask.remove(uuid);
        }

        closed = true;
    }

    /** {@inheritDoc} */
    @Override public boolean wasNull() throws SQLException {
        return wasNull;
    }

    /** {@inheritDoc} */
    @Override public String getString(int colIdx) throws SQLException {
        return getTypedValue(colIdx, String.class);
    }

    /** {@inheritDoc} */
    @Override public boolean getBoolean(int colIdx) throws SQLException {
        Boolean val = getTypedValue(colIdx, Boolean.class);

        return val != null ? val : false;
    }

    /** {@inheritDoc} */
    @Override public byte getByte(int colIdx) throws SQLException {
        Byte val = getTypedValue(colIdx, Byte.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public short getShort(int colIdx) throws SQLException {
        Short val = getTypedValue(colIdx, Short.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public int getInt(int colIdx) throws SQLException {
        Integer val = getTypedValue(colIdx, Integer.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public long getLong(int colIdx) throws SQLException {
        Long val = getTypedValue(colIdx, Long.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public float getFloat(int colIdx) throws SQLException {
        Float val = getTypedValue(colIdx, Float.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public double getDouble(int colIdx) throws SQLException {
        Double val = getTypedValue(colIdx, Double.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public BigDecimal getBigDecimal(int colIdx, int scale) throws SQLException {
        return getTypedValue(colIdx, BigDecimal.class);
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(int colIdx) throws SQLException {
        return getTypedValue(colIdx, byte[].class);
    }

    /** {@inheritDoc} */
    @Override public Date getDate(int colIdx) throws SQLException {
        return getTypedValue(colIdx, Date.class);
    }

    /** {@inheritDoc} */
    @Override public Time getTime(int colIdx) throws SQLException {
        return getTypedValue(colIdx, Time.class);
    }

    /** {@inheritDoc} */
    @Override public Timestamp getTimestamp(int colIdx) throws SQLException {
        return getTypedValue(colIdx, Timestamp.class);
    }

    /** {@inheritDoc} */
    @Override public InputStream getAsciiStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public InputStream getUnicodeStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Stream are not supported.");
    }

    /** {@inheritDoc} */
    @Override public String getString(String colLb) throws SQLException {
        return getTypedValue(colLb, String.class);
    }

    /** {@inheritDoc} */
    @Override public boolean getBoolean(String colLb) throws SQLException {
        Boolean val = getTypedValue(colLb, Boolean.class);

        return val != null ? val : false;
    }

    /** {@inheritDoc} */
    @Override public byte getByte(String colLb) throws SQLException {
        Byte val = getTypedValue(colLb, Byte.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public short getShort(String colLb) throws SQLException {
        Short val = getTypedValue(colLb, Short.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public int getInt(String colLb) throws SQLException {
        Integer val = getTypedValue(colLb, Integer.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public long getLong(String colLb) throws SQLException {
        Long val = getTypedValue(colLb, Long.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public float getFloat(String colLb) throws SQLException {
        Float val = getTypedValue(colLb, Float.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public double getDouble(String colLb) throws SQLException {
        Double val = getTypedValue(colLb, Double.class);

        return val != null ? val : 0;
    }

    /** {@inheritDoc} */
    @Override public BigDecimal getBigDecimal(String colLb, int scale) throws SQLException {
        return getTypedValue(colLb, BigDecimal.class);
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes(String colLb) throws SQLException {
        return getTypedValue(colLb, byte[].class);
    }

    /** {@inheritDoc} */
    @Override public Date getDate(String colLb) throws SQLException {
        return getTypedValue(colLb, Date.class);
    }

    /** {@inheritDoc} */
    @Override public Time getTime(String colLb) throws SQLException {
        return getTypedValue(colLb, Time.class);
    }

    /** {@inheritDoc} */
    @Override public Timestamp getTimestamp(String colLb) throws SQLException {
        return getTypedValue(colLb, Timestamp.class);
    }

    /** {@inheritDoc} */
    @Override public InputStream getAsciiStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public InputStream getUnicodeStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public InputStream getBinaryStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public SQLWarning getWarnings() throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void clearWarnings() throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public String getCursorName() throws SQLException {
        ensureNotClosed();

        return null;
    }

    /** {@inheritDoc} */
    @Override public ResultSetMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        return new JdbcResultSetMetadata(tbls, cols, types);
    }

    /** {@inheritDoc} */
    @Override public Object getObject(int colIdx) throws SQLException {
        return getTypedValue(colIdx, Object.class);
    }

    /** {@inheritDoc} */
    @Override public Object getObject(String colLb) throws SQLException {
        return getTypedValue(colLb, Object.class);
    }

    /** {@inheritDoc} */
    @Override public int findColumn(String colLb) throws SQLException {
        ensureNotClosed();

        int idx = cols.indexOf(colLb.toUpperCase());

        if (idx == -1)
            throw new SQLException("Column not found: " + colLb);

        return idx + 1;
    }

    /** {@inheritDoc} */
    @Override public Reader getCharacterStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Reader getCharacterStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public BigDecimal getBigDecimal(int colIdx) throws SQLException {
        return getTypedValue(colIdx, BigDecimal.class);
    }

    /** {@inheritDoc} */
    @Override public BigDecimal getBigDecimal(String colLb) throws SQLException {
        return getTypedValue(colLb, BigDecimal.class);
    }

    /** {@inheritDoc} */
    @Override public boolean isBeforeFirst() throws SQLException {
        ensureNotClosed();

        return pos < 1;
    }

    /** {@inheritDoc} */
    @Override public boolean isAfterLast() throws SQLException {
        ensureNotClosed();

        return finished && it == null && curr == null;
    }

    /** {@inheritDoc} */
    @Override public boolean isFirst() throws SQLException {
        ensureNotClosed();

        return pos == 1;
    }

    /** {@inheritDoc} */
    @Override public boolean isLast() throws SQLException {
        ensureNotClosed();

        return finished && it == null && curr != null;
    }

    /** {@inheritDoc} */
    @Override public void beforeFirst() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override public void afterLast() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override public boolean first() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override public boolean last() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override public int getRow() throws SQLException {
        ensureNotClosed();

        return isAfterLast() ? 0 : pos;
    }

    /** {@inheritDoc} */
    @Override public boolean absolute(int row) throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override public boolean relative(int rows) throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override public boolean previous() throws SQLException {
        ensureNotClosed();

        throw new SQLException("Result set is forward-only.");
    }

    /** {@inheritDoc} */
    @Override public void setFetchDirection(int direction) throws SQLException {
        ensureNotClosed();

        if (direction != FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException("Only forward direction is supported");
    }

    /** {@inheritDoc} */
    @Override public int getFetchDirection() throws SQLException {
        ensureNotClosed();

        return FETCH_FORWARD;
    }

    /** {@inheritDoc} */
    @Override public void setFetchSize(int fetchSize) throws SQLException {
        ensureNotClosed();

        if (fetchSize <= 0)
            throw new SQLException("Fetch size must be greater than zero.");

        this.fetchSize = fetchSize;
    }

    /** {@inheritDoc} */
    @Override public int getFetchSize() throws SQLException {
        ensureNotClosed();

        return fetchSize;
    }

    /** {@inheritDoc} */
    @Override public int getType() throws SQLException {
        ensureNotClosed();

        return stmt.getResultSetType();
    }

    /** {@inheritDoc} */
    @Override public int getConcurrency() throws SQLException {
        ensureNotClosed();

        return CONCUR_READ_ONLY;
    }

    /** {@inheritDoc} */
    @Override public boolean rowUpdated() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean rowInserted() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean rowDeleted() throws SQLException {
        ensureNotClosed();

        return false;
    }

    /** {@inheritDoc} */
    @Override public void updateNull(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBoolean(int colIdx, boolean x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateByte(int colIdx, byte x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateShort(int colIdx, short x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateInt(int colIdx, int x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateLong(int colIdx, long x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateFloat(int colIdx, float x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateDouble(int colIdx, double x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBigDecimal(int colIdx, BigDecimal x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateString(int colIdx, String x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBytes(int colIdx, byte[] x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateDate(int colIdx, Date x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateTime(int colIdx, Time x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateTimestamp(int colIdx, Timestamp x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateAsciiStream(int colIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBinaryStream(int colIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateCharacterStream(int colIdx, Reader x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateObject(int colIdx, Object x, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateObject(int colIdx, Object x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNull(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBoolean(String colLb, boolean x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateByte(String colLb, byte x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateShort(String colLb, short x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateInt(String colLb, int x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateLong(String colLb, long x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateFloat(String colLb, float x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateDouble(String colLb, double x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBigDecimal(String colLb, BigDecimal x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateString(String colLb, String x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBytes(String colLb, byte[] x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateDate(String colLb, Date x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateTime(String colLb, Time x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateTimestamp(String colLb, Timestamp x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateAsciiStream(String colLb, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBinaryStream(String colLb, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateCharacterStream(String colLb, Reader reader, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateObject(String colLb, Object x, int scaleOrLen) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateObject(String colLb, Object x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void insertRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void deleteRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void refreshRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Row refreshing is not supported.");
    }

    /** {@inheritDoc} */
    @Override public void cancelRowUpdates() throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public void moveToInsertRow() throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void moveToCurrentRow() throws SQLException {
        ensureNotClosed();
    }

    /** {@inheritDoc} */
    @Override public Statement getStatement() throws SQLException {
        ensureNotClosed();

        return stmt;
    }

    /** {@inheritDoc} */
    @Override public Object getObject(int colIdx, Map<String, Class<?>> map) throws SQLException {
        return getTypedValue(colIdx, Object.class);
    }

    /** {@inheritDoc} */
    @Override public Ref getRef(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Blob getBlob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Clob getClob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Array getArray(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Object getObject(String colLb, Map<String, Class<?>> map) throws SQLException {
        return getTypedValue(colLb, Object.class);
    }

    /** {@inheritDoc} */
    @Override public Ref getRef(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Blob getBlob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Clob getClob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Array getArray(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Date getDate(int colIdx, Calendar cal) throws SQLException {
        return getTypedValue(colIdx, Date.class);
    }

    /** {@inheritDoc} */
    @Override public Date getDate(String colLb, Calendar cal) throws SQLException {
        return getTypedValue(colLb, Date.class);
    }

    /** {@inheritDoc} */
    @Override public Time getTime(int colIdx, Calendar cal) throws SQLException {
        return getTypedValue(colIdx, Time.class);
    }

    /** {@inheritDoc} */
    @Override public Time getTime(String colLb, Calendar cal) throws SQLException {
        return getTypedValue(colLb, Time.class);
    }

    /** {@inheritDoc} */
    @Override public Timestamp getTimestamp(int colIdx, Calendar cal) throws SQLException {
        return getTypedValue(colIdx, Timestamp.class);
    }

    /** {@inheritDoc} */
    @Override public Timestamp getTimestamp(String colLb, Calendar cal) throws SQLException {
        return getTypedValue(colLb, Timestamp.class);
    }

    /** {@inheritDoc} */
    @Override public URL getURL(int colIdx) throws SQLException {
        return getTypedValue(colIdx, URL.class);
    }

    /** {@inheritDoc} */
    @Override public URL getURL(String colLb) throws SQLException {
        return getTypedValue(colLb, URL.class);
    }

    /** {@inheritDoc} */
    @Override public void updateRef(int colIdx, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateRef(String colLb, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBlob(int colIdx, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBlob(String colLb, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateClob(int colIdx, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateClob(String colLb, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateArray(int colIdx, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateArray(String colLb, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public RowId getRowId(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public RowId getRowId(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateRowId(int colIdx, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateRowId(String colLb, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public int getHoldability() throws SQLException {
        ensureNotClosed();

        return HOLD_CURSORS_OVER_COMMIT;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
        return closed;
    }

    /** {@inheritDoc} */
    @Override public void updateNString(int colIdx, String nStr) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNString(String colLb, String nStr) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNClob(int colIdx, NClob nClob) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNClob(String colLb, NClob nClob) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public NClob getNClob(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public NClob getNClob(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public SQLXML getSQLXML(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public SQLXML getSQLXML(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateSQLXML(int colIdx, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateSQLXML(String colLb, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public String getNString(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public String getNString(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Reader getNCharacterStream(int colIdx) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public Reader getNCharacterStream(String colLb) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNCharacterStream(int colIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNCharacterStream(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateAsciiStream(int colIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBinaryStream(int colIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateCharacterStream(int colIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateAsciiStream(String colLb, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBinaryStream(String colLb, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateCharacterStream(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBlob(int colIdx, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBlob(String colLb, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateClob(int colIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateClob(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNClob(int colIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNClob(String colLb, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNCharacterStream(int colIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNCharacterStream(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateAsciiStream(int colIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBinaryStream(int colIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateCharacterStream(int colIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateAsciiStream(String colLb, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBinaryStream(String colLb, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateCharacterStream(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBlob(int colIdx, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateBlob(String colLb, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateClob(int colIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateClob(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNClob(int colIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void updateNClob(String colLb, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Updates are not supported.");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Result set is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface == ResultSet.class;
    }

    /** {@inheritDoc} */
    @Override public <T> T getObject(int colIdx, Class<T> type) throws SQLException {
        return getTypedValue(colIdx, type);
    }

    /** {@inheritDoc} */
    @Override public <T> T getObject(String colLb, Class<T> type) throws SQLException {
        return getTypedValue(colLb, type);
    }

    /**
     * Gets casted field value by label.
     *
     * @param colLb Column label.
     * @param cls Value class.
     * @return Casted field value.
     * @throws SQLException In case of error.
     */
    private <T> T getTypedValue(String colLb, Class<T> cls) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        String name = colLb.toUpperCase();

        Integer idx = stmt.fieldsIdxs.get(name);

        int colIdx;

        if (idx != null)
            colIdx = idx;
        else {
            colIdx = cols.indexOf(name) + 1;

            if (colIdx <= 0)
                throw new SQLException("Invalid column label: " + colLb);

            stmt.fieldsIdxs.put(name, colIdx);
        }

        return getTypedValue(colIdx, cls);
    }

    /**
     * Gets casted field value by index.
     *
     * @param colIdx Column index.
     * @param cls Value class.
     * @return Casted field value.
     * @throws SQLException In case of error.
     */
    @SuppressWarnings("unchecked")
    private <T> T getTypedValue(int colIdx, Class<T> cls) throws SQLException {
        ensureNotClosed();
        ensureHasCurrentRow();

        try {
            Object val = curr.get(colIdx - 1);

            wasNull = val == null;

            if (val == null)
                return null;
            else if (cls == String.class)
                return (T)String.valueOf(val);
            else
                return (T)val;
        }
        catch (IndexOutOfBoundsException ignored) {
            throw new SQLException("Invalid column index: " + colIdx);
        }
        catch (ClassCastException ignored) {
            throw new SQLException("Value is an not instance of " + cls.getName());
        }
    }

    /**
     * Ensures that result set is not closed.
     *
     * @throws SQLException If result set is closed.
     */
    private void ensureNotClosed() throws SQLException {
        if (closed)
            throw new SQLException("Result set is closed.");
    }

    /**
     * Ensures that result set is positioned on a row.
     *
     * @throws SQLException If result set is not positioned on a row.
     */
    private void ensureHasCurrentRow() throws SQLException {
        if (curr == null)
            throw new SQLException("Result set is not positioned on a row.");
    }
}
