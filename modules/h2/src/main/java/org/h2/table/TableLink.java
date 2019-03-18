/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.LinkedIndex;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.schema.Schema;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * A linked table contains connection information for a table accessible by
 * JDBC. The table may be stored in a different database.
 */
public class TableLink extends Table {

    private static final int MAX_RETRY = 2;

    private static final long ROW_COUNT_APPROXIMATION = 100_000;

    private final String originalSchema;
    private String driver, url, user, password, originalTable, qualifiedTableName;
    private TableLinkConnection conn;
    private HashMap<String, PreparedStatement> preparedMap = new HashMap<>();
    private final ArrayList<Index> indexes = New.arrayList();
    private final boolean emitUpdates;
    private LinkedIndex linkedIndex;
    private DbException connectException;
    private boolean storesLowerCase;
    private boolean storesMixedCase;
    private boolean storesMixedCaseQuoted;
    private boolean supportsMixedCaseIdentifiers;
    private boolean globalTemporary;
    private boolean readOnly;

    public TableLink(Schema schema, int id, String name, String driver,
            String url, String user, String password, String originalSchema,
            String originalTable, boolean emitUpdates, boolean force) {
        super(schema, id, name, false, true);
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        this.originalSchema = originalSchema;
        this.originalTable = originalTable;
        this.emitUpdates = emitUpdates;
        try {
            connect();
        } catch (DbException e) {
            if (!force) {
                throw e;
            }
            Column[] cols = { };
            setColumns(cols);
            linkedIndex = new LinkedIndex(this, id, IndexColumn.wrap(cols),
                    IndexType.createNonUnique(false));
            indexes.add(linkedIndex);
        }
    }

    private void connect() {
        connectException = null;
        for (int retry = 0;; retry++) {
            try {
                conn = database.getLinkConnection(driver, url, user, password);
                synchronized (conn) {
                    try {
                        readMetaData();
                        return;
                    } catch (Exception e) {
                        // could be SQLException or RuntimeException
                        conn.close(true);
                        conn = null;
                        throw DbException.convert(e);
                    }
                }
            } catch (DbException e) {
                if (retry >= MAX_RETRY) {
                    connectException = e;
                    throw e;
                }
            }
        }
    }

    private void readMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getConnection().getMetaData();
        storesLowerCase = meta.storesLowerCaseIdentifiers();
        storesMixedCase = meta.storesMixedCaseIdentifiers();
        storesMixedCaseQuoted = meta.storesMixedCaseQuotedIdentifiers();
        supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();
        ResultSet rs = meta.getTables(null, originalSchema, originalTable, null);
        if (rs.next() && rs.next()) {
            throw DbException.get(ErrorCode.SCHEMA_NAME_MUST_MATCH, originalTable);
        }
        rs.close();
        rs = meta.getColumns(null, originalSchema, originalTable, null);
        int i = 0;
        ArrayList<Column> columnList = New.arrayList();
        HashMap<String, Column> columnMap = new HashMap<>();
        String catalog = null, schema = null;
        while (rs.next()) {
            String thisCatalog = rs.getString("TABLE_CAT");
            if (catalog == null) {
                catalog = thisCatalog;
            }
            String thisSchema = rs.getString("TABLE_SCHEM");
            if (schema == null) {
                schema = thisSchema;
            }
            if (!Objects.equals(catalog, thisCatalog) ||
                    !Objects.equals(schema, thisSchema)) {
                // if the table exists in multiple schemas or tables,
                // use the alternative solution
                columnMap.clear();
                columnList.clear();
                break;
            }
            String n = rs.getString("COLUMN_NAME");
            n = convertColumnName(n);
            int sqlType = rs.getInt("DATA_TYPE");
            String sqlTypeName = rs.getString("TYPE_NAME");
            long precision = rs.getInt("COLUMN_SIZE");
            precision = convertPrecision(sqlType, precision);
            int scale = rs.getInt("DECIMAL_DIGITS");
            scale = convertScale(sqlType, scale);
            int displaySize = MathUtils.convertLongToInt(precision);
            int type = DataType.convertSQLTypeToValueType(sqlType, sqlTypeName);
            Column col = new Column(n, type, precision, scale, displaySize);
            col.setTable(this, i++);
            columnList.add(col);
            columnMap.put(n, col);
        }
        rs.close();
        if (originalTable.indexOf('.') < 0 && !StringUtils.isNullOrEmpty(schema)) {
            qualifiedTableName = schema + "." + originalTable;
        } else {
            qualifiedTableName = originalTable;
        }
        // check if the table is accessible

        try (Statement stat = conn.getConnection().createStatement()) {
            rs = stat.executeQuery("SELECT * FROM " +
                    qualifiedTableName + " T WHERE 1=0");
            if (columnList.isEmpty()) {
                // alternative solution
                ResultSetMetaData rsMeta = rs.getMetaData();
                for (i = 0; i < rsMeta.getColumnCount();) {
                    String n = rsMeta.getColumnName(i + 1);
                    n = convertColumnName(n);
                    int sqlType = rsMeta.getColumnType(i + 1);
                    long precision = rsMeta.getPrecision(i + 1);
                    precision = convertPrecision(sqlType, precision);
                    int scale = rsMeta.getScale(i + 1);
                    scale = convertScale(sqlType, scale);
                    int displaySize = rsMeta.getColumnDisplaySize(i + 1);
                    int type = DataType.getValueTypeFromResultSet(rsMeta, i + 1);
                    Column col = new Column(n, type, precision, scale, displaySize);
                    col.setTable(this, i++);
                    columnList.add(col);
                    columnMap.put(n, col);
                }
            }
            rs.close();
        } catch (Exception e) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, e,
                    originalTable + "(" + e.toString() + ")");
        }
        Column[] cols = columnList.toArray(new Column[0]);
        setColumns(cols);
        int id = getId();
        linkedIndex = new LinkedIndex(this, id, IndexColumn.wrap(cols),
                IndexType.createNonUnique(false));
        indexes.add(linkedIndex);
        try {
            rs = meta.getPrimaryKeys(null, originalSchema, originalTable);
        } catch (Exception e) {
            // Some ODBC bridge drivers don't support it:
            // some combinations of "DataDirect SequeLink(R) for JDBC"
            // http://www.datadirect.com/index.ssp
            rs = null;
        }
        String pkName = "";
        ArrayList<Column> list;
        if (rs != null && rs.next()) {
            // the problem is, the rows are not sorted by KEY_SEQ
            list = New.arrayList();
            do {
                int idx = rs.getInt("KEY_SEQ");
                if (pkName == null) {
                    pkName = rs.getString("PK_NAME");
                }
                while (list.size() < idx) {
                    list.add(null);
                }
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                if (idx == 0) {
                    // workaround for a bug in the SQLite JDBC driver
                    list.add(column);
                } else {
                    list.set(idx - 1, column);
                }
            } while (rs.next());
            addIndex(list, IndexType.createPrimaryKey(false, false));
            rs.close();
        }
        try {
            rs = meta.getIndexInfo(null, originalSchema, originalTable, false, true);
        } catch (Exception e) {
            // Oracle throws an exception if the table is not found or is a
            // SYNONYM
            rs = null;
        }
        String indexName = null;
        list = New.arrayList();
        IndexType indexType = null;
        if (rs != null) {
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    // ignore index statistics
                    continue;
                }
                String newIndex = rs.getString("INDEX_NAME");
                if (pkName.equals(newIndex)) {
                    continue;
                }
                if (indexName != null && !indexName.equals(newIndex)) {
                    addIndex(list, indexType);
                    indexName = null;
                }
                if (indexName == null) {
                    indexName = newIndex;
                    list.clear();
                }
                boolean unique = !rs.getBoolean("NON_UNIQUE");
                indexType = unique ? IndexType.createUnique(false, false) :
                        IndexType.createNonUnique(false);
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                list.add(column);
            }
            rs.close();
        }
        if (indexName != null) {
            addIndex(list, indexType);
        }
    }

    private static long convertPrecision(int sqlType, long precision) {
        // workaround for an Oracle problem:
        // for DATE columns, the reported precision is 7
        // for DECIMAL columns, the reported precision is 0
        switch (sqlType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            if (precision == 0) {
                precision = 65535;
            }
            break;
        case Types.DATE:
            precision = Math.max(ValueDate.PRECISION, precision);
            break;
        case Types.TIMESTAMP:
            precision = Math.max(ValueTimestamp.MAXIMUM_PRECISION, precision);
            break;
        case Types.TIME:
            precision = Math.max(ValueTime.MAXIMUM_PRECISION, precision);
            break;
        }
        return precision;
    }

    private static int convertScale(int sqlType, int scale) {
        // workaround for an Oracle problem:
        // for DECIMAL columns, the reported precision is -127
        switch (sqlType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            if (scale < 0) {
                scale = 32767;
            }
            break;
        }
        return scale;
    }

    private String convertColumnName(String columnName) {
        if ((storesMixedCase || storesLowerCase) &&
                columnName.equals(StringUtils.toLowerEnglish(columnName))) {
            columnName = StringUtils.toUpperEnglish(columnName);
        } else if (storesMixedCase && !supportsMixedCaseIdentifiers) {
            // TeraData
            columnName = StringUtils.toUpperEnglish(columnName);
        } else if (storesMixedCase && storesMixedCaseQuoted) {
            // MS SQL Server (identifiers are case insensitive even if quoted)
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        return columnName;
    }

    private void addIndex(List<Column> list, IndexType indexType) {
        // bind the index to the leading recognized columns in the index
        // (null columns might come from a function-based index)
        int firstNull = list.indexOf(null);
        if (firstNull == 0) {
            trace.info("Omitting linked index - no recognized columns.");
            return;
        } else if (firstNull > 0) {
            trace.info("Unrecognized columns in linked index. " +
                    "Registering the index against the leading {0} " +
                    "recognized columns of {1} total columns.", firstNull, list.size());
            list = list.subList(0, firstNull);
        }
        Column[] cols = list.toArray(new Column[0]);
        Index index = new LinkedIndex(this, 0, IndexColumn.wrap(cols), indexType);
        indexes.add(index);
    }

    @Override
    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("CREATE FORCE ");
        if (isTemporary()) {
            if (globalTemporary) {
                buff.append("GLOBAL ");
            } else {
                buff.append("LOCAL ");
            }
            buff.append("TEMPORARY ");
        }
        buff.append("LINKED TABLE ").append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append('(').
            append(StringUtils.quoteStringSQL(driver)).
            append(", ").
            append(StringUtils.quoteStringSQL(url)).
            append(", ").
            append(StringUtils.quoteStringSQL(user)).
            append(", ").
            append(StringUtils.quoteStringSQL(password)).
            append(", ").
            append(StringUtils.quoteStringSQL(originalTable)).
            append(')');
        if (emitUpdates) {
            buff.append(" EMIT UPDATES");
        }
        if (readOnly) {
            buff.append(" READONLY");
        }
        buff.append(" /*").append(JdbcSQLException.HIDE_SQL).append("*/");
        return buff.toString();
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId,
            IndexColumn[] cols, IndexType indexType, boolean create,
            String indexComment) {
        throw DbException.getUnsupportedException("LINK");
    }

    @Override
    public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
        // nothing to do
        return false;
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    @Override
    public Index getScanIndex(Session session) {
        return linkedIndex;
    }

    private void checkReadOnly() {
        if (readOnly) {
            throw DbException.get(ErrorCode.DATABASE_IS_READ_ONLY);
        }
    }

    @Override
    public void removeRow(Session session, Row row) {
        checkReadOnly();
        getScanIndex(session).remove(session, row);
    }

    @Override
    public void addRow(Session session, Row row) {
        checkReadOnly();
        getScanIndex(session).add(session, row);
    }

    @Override
    public void close(Session session) {
        if (conn != null) {
            try {
                conn.close(false);
            } finally {
                conn = null;
            }
        }
    }

    @Override
    public synchronized long getRowCount(Session session) {
        //The foo alias is used to support the PostgreSQL syntax
        String sql = "SELECT COUNT(*) FROM " + qualifiedTableName + " as foo";
        try {
            PreparedStatement prep = execute(sql, null, false);
            ResultSet rs = prep.getResultSet();
            rs.next();
            long count = rs.getLong(1);
            rs.close();
            reusePreparedStatement(prep, sql);
            return count;
        } catch (Exception e) {
            throw wrapException(sql, e);
        }
    }

    /**
     * Wrap a SQL exception that occurred while accessing a linked table.
     *
     * @param sql the SQL statement
     * @param ex the exception from the remote database
     * @return the wrapped exception
     */
    public static DbException wrapException(String sql, Exception ex) {
        SQLException e = DbException.toSQLException(ex);
        return DbException.get(ErrorCode.ERROR_ACCESSING_LINKED_TABLE_2,
                e, sql, e.toString());
    }

    public String getQualifiedTable() {
        return qualifiedTableName;
    }

    /**
     * Execute a SQL statement using the given parameters. Prepared
     * statements are kept in a hash map to avoid re-creating them.
     *
     * @param sql the SQL statement
     * @param params the parameters or null
     * @param reusePrepared if the prepared statement can be re-used immediately
     * @return the prepared statement, or null if it is re-used
     */
    public PreparedStatement execute(String sql, ArrayList<Value> params,
            boolean reusePrepared) {
        if (conn == null) {
            throw connectException;
        }
        for (int retry = 0;; retry++) {
            try {
                synchronized (conn) {
                    PreparedStatement prep = preparedMap.remove(sql);
                    if (prep == null) {
                        prep = conn.getConnection().prepareStatement(sql);
                    }
                    if (trace.isDebugEnabled()) {
                        StatementBuilder buff = new StatementBuilder();
                        buff.append(getName()).append(":\n").append(sql);
                        if (params != null && !params.isEmpty()) {
                            buff.append(" {");
                            int i = 1;
                            for (Value v : params) {
                                buff.appendExceptFirst(", ");
                                buff.append(i++).append(": ").append(v.getSQL());
                            }
                            buff.append('}');
                        }
                        buff.append(';');
                        trace.debug(buff.toString());
                    }
                    if (params != null) {
                        for (int i = 0, size = params.size(); i < size; i++) {
                            Value v = params.get(i);
                            v.set(prep, i + 1);
                        }
                    }
                    prep.execute();
                    if (reusePrepared) {
                        reusePreparedStatement(prep, sql);
                        return null;
                    }
                    return prep;
                }
            } catch (SQLException e) {
                if (retry >= MAX_RETRY) {
                    throw DbException.convert(e);
                }
                conn.close(true);
                connect();
            }
        }
    }

    @Override
    public void unlock(Session s) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public void checkSupportAlter() {
        throw DbException.getUnsupportedException("LINK");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("LINK");
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public TableType getTableType() {
        return TableType.TABLE_LINK;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        super.removeChildrenAndResources(session);
        close(session);
        database.removeMeta(session, getId());
        driver = null;
        url = user = password = originalTable = null;
        preparedMap = null;
        invalidate();
    }

    public boolean isOracle() {
        return url.startsWith("jdbc:oracle:");
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {
        // data may have been modified externally
        return Long.MAX_VALUE;
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public void updateRows(Prepared prepared, Session session, RowList rows) {
        boolean deleteInsert;
        checkReadOnly();
        if (emitUpdates) {
            for (rows.reset(); rows.hasNext();) {
                prepared.checkCanceled();
                Row oldRow = rows.next();
                Row newRow = rows.next();
                linkedIndex.update(oldRow, newRow);
                session.log(this, UndoLogRecord.DELETE, oldRow);
                session.log(this, UndoLogRecord.INSERT, newRow);
            }
            deleteInsert = false;
        } else {
            deleteInsert = true;
        }
        if (deleteInsert) {
            super.updateRows(prepared, session, rows);
        }
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        this.globalTemporary = globalTemporary;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    /**
     * Add this prepared statement to the list of cached statements.
     *
     * @param prep the prepared statement
     * @param sql the SQL statement
     */
    public void reusePreparedStatement(PreparedStatement prep, String sql) {
        synchronized (conn) {
            preparedMap.put(sql, prep);
        }
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    /**
     * Linked tables don't know if they are readonly. This overwrites
     * the default handling.
     */
    @Override
    public void checkWritingAllowed() {
        // only the target database can verify this
    }

    /**
     * Convert the values if required. Default values are not set (kept as
     * null).
     *
     * @param session the session
     * @param row the row
     */
    @Override
    public void validateConvertUpdateSequence(Session session, Row row) {
        for (int i = 0; i < columns.length; i++) {
            Value value = row.getValue(i);
            if (value != null) {
                // null means use the default value
                Column column = columns[i];
                Value v2 = column.validateConvertUpdateSequence(session, value);
                if (v2 != value) {
                    row.setValue(i, v2);
                }
            }
        }
    }

    /**
     * Get or generate a default value for the given column. Default values are
     * not set (kept as null).
     *
     * @param session the session
     * @param column the column
     * @return the value
     */
    @Override
    public Value getDefaultValue(Session session, Column column) {
        return null;
    }

}
