/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import org.h2.api.Trigger;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ValueExpression;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * This class implements the native full text search.
 * Most methods can be called using SQL statements as well.
 */
public class FullText {

    /**
     * A column name of the result set returned by the searchData method.
     */
    private static final String FIELD_SCHEMA = "SCHEMA";

    /**
     * A column name of the result set returned by the searchData method.
     */
    private static final String FIELD_TABLE = "TABLE";

    /**
     * A column name of the result set returned by the searchData method.
     */
    private static final String FIELD_COLUMNS = "COLUMNS";

    /**
     * A column name of the result set returned by the searchData method.
     */
    private static final String FIELD_KEYS = "KEYS";

    /**
     * The hit score.
     */
    private static final String FIELD_SCORE = "SCORE";

    private static final String TRIGGER_PREFIX = "FT_";
    private static final String SCHEMA = "FT";
    private static final String SELECT_MAP_BY_WORD_ID =
            "SELECT ROWID FROM " + SCHEMA + ".MAP WHERE WORDID=?";
    private static final String SELECT_ROW_BY_ID =
            "SELECT KEY, INDEXID FROM " + SCHEMA + ".ROWS WHERE ID=?";

    /**
     * The column name of the result set returned by the search method.
     */
    private static final String FIELD_QUERY = "QUERY";

    /**
     * Initializes full text search functionality for this database. This adds
     * the following Java functions to the database:
     * <ul>
     * <li>FT_CREATE_INDEX(schemaNameString, tableNameString,
     * columnListString)</li>
     * <li>FT_SEARCH(queryString, limitInt, offsetInt): result set</li>
     * <li>FT_REINDEX()</li>
     * <li>FT_DROP_ALL()</li>
     * </ul>
     * It also adds a schema FT to the database where bookkeeping information
     * is stored. This function may be called from a Java application, or by
     * using the SQL statements:
     *
     * <pre>
     * CREATE ALIAS IF NOT EXISTS FT_INIT FOR
     *      &quot;org.h2.fulltext.FullText.init&quot;;
     * CALL FT_INIT();
     * </pre>
     *
     * @param conn the connection
     */
    public static void init(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA +
                ".INDEXES(ID INT AUTO_INCREMENT PRIMARY KEY, " +
                "SCHEMA VARCHAR, TABLE VARCHAR, COLUMNS VARCHAR, " +
                "UNIQUE(SCHEMA, TABLE))");
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA +
                ".WORDS(ID INT AUTO_INCREMENT PRIMARY KEY, " +
                "NAME VARCHAR, UNIQUE(NAME))");
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA +
                ".ROWS(ID IDENTITY, HASH INT, INDEXID INT, " +
                "KEY VARCHAR, UNIQUE(HASH, INDEXID, KEY))");
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA +
                ".MAP(ROWID INT, WORDID INT, PRIMARY KEY(WORDID, ROWID))");
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA +
                ".IGNORELIST(LIST VARCHAR)");
        stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA +
                ".SETTINGS(KEY VARCHAR PRIMARY KEY, VALUE VARCHAR)");
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_CREATE_INDEX FOR \"" +
                FullText.class.getName() + ".createIndex\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_DROP_INDEX FOR \"" +
                FullText.class.getName() + ".dropIndex\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_SEARCH FOR \"" +
                FullText.class.getName() + ".search\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_SEARCH_DATA FOR \"" +
                FullText.class.getName() + ".searchData\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_REINDEX FOR \"" +
                FullText.class.getName() + ".reindex\"");
        stat.execute("CREATE ALIAS IF NOT EXISTS FT_DROP_ALL FOR \"" +
                FullText.class.getName() + ".dropAll\"");
        FullTextSettings setting = FullTextSettings.getInstance(conn);
        ResultSet rs = stat.executeQuery("SELECT * FROM " + SCHEMA +
                ".IGNORELIST");
        while (rs.next()) {
            String commaSeparatedList = rs.getString(1);
            setIgnoreList(setting, commaSeparatedList);
        }
        rs = stat.executeQuery("SELECT * FROM " + SCHEMA + ".SETTINGS");
        while (rs.next()) {
            String key = rs.getString(1);
            if ("whitespaceChars".equals(key)) {
                String value = rs.getString(2);
                setting.setWhitespaceChars(value);
            }
        }
        rs = stat.executeQuery("SELECT * FROM " + SCHEMA + ".WORDS");
        while (rs.next()) {
            String word = rs.getString("NAME");
            int id = rs.getInt("ID");
            word = setting.convertWord(word);
            if (word != null) {
                setting.addWord(word, id);
            }
        }
        setting.setInitialized(true);
    }

    /**
     * Create a new full text index for a table and column list. Each table may
     * only have one index at any time.
     *
     * @param conn the connection
     * @param schema the schema name of the table (case sensitive)
     * @param table the table name (case sensitive)
     * @param columnList the column list (null for all columns)
     */
    public static void createIndex(Connection conn, String schema,
            String table, String columnList) throws SQLException {
        init(conn);
        PreparedStatement prep = conn.prepareStatement("INSERT INTO " + SCHEMA
                + ".INDEXES(SCHEMA, TABLE, COLUMNS) VALUES(?, ?, ?)");
        prep.setString(1, schema);
        prep.setString(2, table);
        prep.setString(3, columnList);
        prep.execute();
        createTrigger(conn, schema, table);
        indexExistingRows(conn, schema, table);
    }

    /**
     * Re-creates the full text index for this database. Calling this method is
     * usually not needed, as the index is kept up-to-date automatically.
     *
     * @param conn the connection
     */
    public static void reindex(Connection conn) throws SQLException {
        init(conn);
        removeAllTriggers(conn, TRIGGER_PREFIX);
        FullTextSettings setting = FullTextSettings.getInstance(conn);
        setting.clearWordList();
        Statement stat = conn.createStatement();
        stat.execute("TRUNCATE TABLE " + SCHEMA + ".WORDS");
        stat.execute("TRUNCATE TABLE " + SCHEMA + ".ROWS");
        stat.execute("TRUNCATE TABLE " + SCHEMA + ".MAP");
        ResultSet rs = stat.executeQuery("SELECT * FROM " + SCHEMA + ".INDEXES");
        while (rs.next()) {
            String schema = rs.getString("SCHEMA");
            String table = rs.getString("TABLE");
            createTrigger(conn, schema, table);
            indexExistingRows(conn, schema, table);
        }
    }

    /**
     * Drop an existing full text index for a table. This method returns
     * silently if no index for this table exists.
     *
     * @param conn the connection
     * @param schema the schema name of the table (case sensitive)
     * @param table the table name (case sensitive)
     */
    public static void dropIndex(Connection conn, String schema, String table)
            throws SQLException {
        init(conn);
        PreparedStatement prep = conn.prepareStatement("SELECT ID FROM " + SCHEMA
                + ".INDEXES WHERE SCHEMA=? AND TABLE=?");
        prep.setString(1, schema);
        prep.setString(2, table);
        ResultSet rs = prep.executeQuery();
        if (!rs.next()) {
            return;
        }
        int indexId = rs.getInt(1);
        prep = conn.prepareStatement("DELETE FROM " + SCHEMA
                + ".INDEXES WHERE ID=?");
        prep.setInt(1, indexId);
        prep.execute();
        createOrDropTrigger(conn, schema, table, false);
        prep = conn.prepareStatement("DELETE FROM " + SCHEMA +
                ".ROWS WHERE INDEXID=? AND ROWNUM<10000");
        while (true) {
            prep.setInt(1, indexId);
            int deleted = prep.executeUpdate();
            if (deleted == 0) {
                break;
            }
        }
        prep = conn.prepareStatement("DELETE FROM " + SCHEMA + ".MAP " +
                "WHERE NOT EXISTS (SELECT * FROM " + SCHEMA +
                ".ROWS R WHERE R.ID=ROWID) AND ROWID<10000");
        while (true) {
            int deleted = prep.executeUpdate();
            if (deleted == 0) {
                break;
            }
        }
    }

    /**
     * Drops all full text indexes from the database.
     *
     * @param conn the connection
     */
    public static void dropAll(Connection conn) throws SQLException {
        init(conn);
        Statement stat = conn.createStatement();
        stat.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        removeAllTriggers(conn, TRIGGER_PREFIX);
        FullTextSettings setting = FullTextSettings.getInstance(conn);
        setting.removeAllIndexes();
        setting.clearIgnored();
        setting.clearWordList();
    }

    /**
     * Searches from the full text index for this database.
     * The returned result set has the following column:
     * <ul><li>QUERY (varchar): the query to use to get the data.
     * The query does not include 'SELECT * FROM '. Example:
     * PUBLIC.TEST WHERE ID = 1
     * </li><li>SCORE (float) the relevance score. This value is always 1.0
     * for the native fulltext search.
     * </li></ul>
     *
     * @param conn the connection
     * @param text the search query
     * @param limit the maximum number of rows or 0 for no limit
     * @param offset the offset or 0 for no offset
     * @return the result set
     */
    public static ResultSet search(Connection conn, String text, int limit,
            int offset) throws SQLException {
        try {
            return search(conn, text, limit, offset, false);
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * Searches from the full text index for this database. The result contains
     * the primary key data as an array. The returned result set has the
     * following columns:
     * <ul>
     * <li>SCHEMA (varchar): the schema name. Example: PUBLIC </li>
     * <li>TABLE (varchar): the table name. Example: TEST </li>
     * <li>COLUMNS (array of varchar): comma separated list of quoted column
     * names. The column names are quoted if necessary. Example: (ID) </li>
     * <li>KEYS (array of values): comma separated list of values. Example: (1)
     * </li>
     * <li>SCORE (float) the relevance score. This value is always 1.0
     * for the native fulltext search.
     * </li>
     * </ul>
     *
     * @param conn the connection
     * @param text the search query
     * @param limit the maximum number of rows or 0 for no limit
     * @param offset the offset or 0 for no offset
     * @return the result set
     */
    public static ResultSet searchData(Connection conn, String text, int limit,
            int offset) throws SQLException {
        try {
            return search(conn, text, limit, offset, true);
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * Change the ignore list. The ignore list is a comma separated list of
     * common words that must not be indexed. The default ignore list is empty.
     * If indexes already exist at the time this list is changed, reindex must
     * be called.
     *
     * @param conn the connection
     * @param commaSeparatedList the list
     */
    public static void setIgnoreList(Connection conn, String commaSeparatedList)
            throws SQLException {
        try {
            init(conn);
            FullTextSettings setting = FullTextSettings.getInstance(conn);
            setIgnoreList(setting, commaSeparatedList);
            Statement stat = conn.createStatement();
            stat.execute("TRUNCATE TABLE " + SCHEMA + ".IGNORELIST");
            PreparedStatement prep = conn.prepareStatement("INSERT INTO " +
                    SCHEMA + ".IGNORELIST VALUES(?)");
            prep.setString(1, commaSeparatedList);
            prep.execute();
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * Change the whitespace characters. The whitespace characters are used to
     * separate words. If indexes already exist at the time this list is
     * changed, reindex must be called.
     *
     * @param conn the connection
     * @param whitespaceChars the list of characters
     */
    public static void setWhitespaceChars(Connection conn,
            String whitespaceChars) throws SQLException {
        try {
            init(conn);
            FullTextSettings setting = FullTextSettings.getInstance(conn);
            setting.setWhitespaceChars(whitespaceChars);
            PreparedStatement prep = conn.prepareStatement("MERGE INTO " +
                    SCHEMA + ".SETTINGS VALUES(?, ?)");
            prep.setString(1, "whitespaceChars");
            prep.setString(2, whitespaceChars);
            prep.execute();
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * INTERNAL.
     * Convert the object to a string.
     *
     * @param data the object
     * @param type the SQL type
     * @return the string
     */
    protected static String asString(Object data, int type) throws SQLException {
        if (data == null) {
            return "NULL";
        }
        switch (type) {
        case Types.BIT:
        case Types.BOOLEAN:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.NUMERIC:
        case Types.REAL:
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.LONGVARCHAR:
        case Types.CHAR:
        case Types.VARCHAR:
            return data.toString();
        case Types.CLOB:
            try {
                if (data instanceof Clob) {
                    data = ((Clob) data).getCharacterStream();
                }
                return IOUtils.readStringAndClose((Reader) data, -1);
            } catch (IOException e) {
                throw DbException.toSQLException(e);
            }
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BINARY:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        case Types.BLOB:
        case Types.STRUCT:
        case Types.REF:
        case Types.NULL:
        case Types.ARRAY:
        case Types.DATALINK:
        case Types.DISTINCT:
            throw throwException("Unsupported column data type: " + type);
        default:
            return "";
        }
    }

    /**
     * Create an empty search result and initialize the columns.
     *
     * @param data true if the result set should contain the primary key data as
     *            an array.
     * @return the empty result set
     */
    protected static SimpleResultSet createResultSet(boolean data) {
        SimpleResultSet result = new SimpleResultSet();
        if (data) {
            result.addColumn(FullText.FIELD_SCHEMA, Types.VARCHAR, 0, 0);
            result.addColumn(FullText.FIELD_TABLE, Types.VARCHAR, 0, 0);
            result.addColumn(FullText.FIELD_COLUMNS, Types.ARRAY, 0, 0);
            result.addColumn(FullText.FIELD_KEYS, Types.ARRAY, 0, 0);
        } else {
            result.addColumn(FullText.FIELD_QUERY, Types.VARCHAR, 0, 0);
        }
        result.addColumn(FullText.FIELD_SCORE, Types.FLOAT, 0, 0);
        return result;
    }

    /**
     * Parse a primary key condition into the primary key columns.
     *
     * @param conn the database connection
     * @param key the primary key condition as a string
     * @return an array containing the column name list and the data list
     */
    protected static Object[][] parseKey(Connection conn, String key) {
        ArrayList<String> columns = New.arrayList();
        ArrayList<String> data = New.arrayList();
        JdbcConnection c = (JdbcConnection) conn;
        Session session = (Session) c.getSession();
        Parser p = new Parser(session);
        Expression expr = p.parseExpression(key);
        addColumnData(columns, data, expr);
        Object[] col = columns.toArray();
        Object[] dat = data.toArray();
        Object[][] columnData = { col, dat };
        return columnData;
    }

    /**
     * INTERNAL.
     * Convert an object to a String as used in a SQL statement.
     *
     * @param data the object
     * @param type the SQL type
     * @return the SQL String
     */
    protected static String quoteSQL(Object data, int type) throws SQLException {
        if (data == null) {
            return "NULL";
        }
        switch (type) {
        case Types.BIT:
        case Types.BOOLEAN:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.DOUBLE:
        case Types.FLOAT:
        case Types.NUMERIC:
        case Types.REAL:
        case Types.SMALLINT:
        case Types.TINYINT:
            return data.toString();
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
        case Types.LONGVARCHAR:
        case Types.CHAR:
        case Types.VARCHAR:
            return quoteString(data.toString());
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BINARY:
            if (data instanceof UUID) {
                return "'" + data.toString() + "'";
            }
            return "'" + StringUtils.convertBytesToHex((byte[]) data) + "'";
        case Types.CLOB:
        case Types.JAVA_OBJECT:
        case Types.OTHER:
        case Types.BLOB:
        case Types.STRUCT:
        case Types.REF:
        case Types.NULL:
        case Types.ARRAY:
        case Types.DATALINK:
        case Types.DISTINCT:
            throw throwException("Unsupported key data type: " + type);
        default:
            return "";
        }
    }

    /**
     * Remove all triggers that start with the given prefix.
     *
     * @param conn the database connection
     * @param prefix the prefix
     */
    protected static void removeAllTriggers(Connection conn, String prefix)
            throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TRIGGERS");
        Statement stat2 = conn.createStatement();
        while (rs.next()) {
            String schema = rs.getString("TRIGGER_SCHEMA");
            String name = rs.getString("TRIGGER_NAME");
            if (name.startsWith(prefix)) {
                name = StringUtils.quoteIdentifier(schema) + "." +
                        StringUtils.quoteIdentifier(name);
                stat2.execute("DROP TRIGGER " + name);
            }
        }
    }

    /**
     * Set the column indices of a set of keys.
     *
     * @param index the column indices (will be modified)
     * @param keys the key list
     * @param columns the column list
     */
    protected static void setColumns(int[] index, ArrayList<String> keys,
            ArrayList<String> columns) throws SQLException {
        for (int i = 0, keySize = keys.size(); i < keySize; i++) {
            String key = keys.get(i);
            int found = -1;
            int columnsSize = columns.size();
            for (int j = 0; found == -1 && j < columnsSize; j++) {
                String column = columns.get(j);
                if (column.equals(key)) {
                    found = j;
                }
            }
            if (found < 0) {
                throw throwException("Column not found: " + key);
            }
            index[i] = found;
        }
    }

    /**
     * Do the search.
     *
     * @param conn the database connection
     * @param text the query
     * @param limit the limit
     * @param offset the offset
     * @param data whether the raw data should be returned
     * @return the result set
     */
    protected static ResultSet search(Connection conn, String text, int limit,
            int offset, boolean data) throws SQLException {
        SimpleResultSet result = createResultSet(data);
        if (conn.getMetaData().getURL().startsWith("jdbc:columnlist:")) {
            // this is just to query the result set columns
            return result;
        }
        if (text == null || text.trim().length() == 0) {
            return result;
        }
        FullTextSettings setting = FullTextSettings.getInstance(conn);
        if (!setting.isInitialized()) {
            init(conn);
        }
        Set<String> words = new HashSet<>();
        addWords(setting, words, text);
        Set<Integer> rIds = null, lastRowIds;

        PreparedStatement prepSelectMapByWordId = setting.prepare(conn,
                SELECT_MAP_BY_WORD_ID);
        for (String word : words) {
            lastRowIds = rIds;
            rIds = new HashSet<>();
            Integer wId = setting.getWordId(word);
            if (wId == null) {
                continue;
            }
            prepSelectMapByWordId.setInt(1, wId.intValue());
            ResultSet rs = prepSelectMapByWordId.executeQuery();
            while (rs.next()) {
                Integer rId = rs.getInt(1);
                if (lastRowIds == null || lastRowIds.contains(rId)) {
                    rIds.add(rId);
                }
            }
        }
        if (rIds == null || rIds.isEmpty()) {
            return result;
        }
        PreparedStatement prepSelectRowById = setting.prepare(conn, SELECT_ROW_BY_ID);
        int rowCount = 0;
        for (int rowId : rIds) {
            prepSelectRowById.setInt(1, rowId);
            ResultSet rs = prepSelectRowById.executeQuery();
            if (!rs.next()) {
                continue;
            }
            if (offset > 0) {
                offset--;
            } else {
                String key = rs.getString(1);
                int indexId = rs.getInt(2);
                IndexInfo index = setting.getIndexInfo(indexId);
                if (data) {
                    Object[][] columnData = parseKey(conn, key);
                    result.addRow(
                            index.schema,
                            index.table,
                            columnData[0],
                            columnData[1],
                            1.0);
                } else {
                    String query = StringUtils.quoteIdentifier(index.schema) +
                        "." + StringUtils.quoteIdentifier(index.table) +
                        " WHERE " + key;
                    result.addRow(query, 1.0);
                }
                rowCount++;
                if (limit > 0 && rowCount >= limit) {
                    break;
                }
            }
        }
        return result;
    }

    private static void addColumnData(ArrayList<String> columns,
            ArrayList<String> data, Expression expr) {
        if (expr instanceof ConditionAndOr) {
            ConditionAndOr and = (ConditionAndOr) expr;
            Expression left = and.getExpression(true);
            Expression right = and.getExpression(false);
            addColumnData(columns, data, left);
            addColumnData(columns, data, right);
        } else {
            Comparison comp = (Comparison) expr;
            ExpressionColumn ec = (ExpressionColumn) comp.getExpression(true);
            ValueExpression ev = (ValueExpression) comp.getExpression(false);
            String columnName = ec.getColumnName();
            columns.add(columnName);
            if (ev == null) {
                data.add(null);
            } else {
                data.add(ev.getValue(null).getString());
            }
        }
    }

    /**
     * Add all words in the given text to the hash set.
     *
     * @param setting the fulltext settings
     * @param set the hash set
     * @param reader the reader
     */
    protected static void addWords(FullTextSettings setting,
            Set<String> set, Reader reader) {
        StreamTokenizer tokenizer = new StreamTokenizer(reader);
        tokenizer.resetSyntax();
        tokenizer.wordChars(' ' + 1, 255);
        char[] whitespaceChars = setting.getWhitespaceChars().toCharArray();
        for (char ch : whitespaceChars) {
            tokenizer.whitespaceChars(ch, ch);
        }
        try {
            while (true) {
                int token = tokenizer.nextToken();
                if (token == StreamTokenizer.TT_EOF) {
                    break;
                } else if (token == StreamTokenizer.TT_WORD) {
                    String word = tokenizer.sval;
                    word = setting.convertWord(word);
                    if (word != null) {
                        set.add(word);
                    }
                }
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Tokenizer error");
        }
    }

    /**
     * Add all words in the given text to the hash set.
     *
     * @param setting the fulltext settings
     * @param set the hash set
     * @param text the text
     */
    protected static void addWords(FullTextSettings setting,
            Set<String> set, String text) {
        String whitespaceChars = setting.getWhitespaceChars();
        StringTokenizer tokenizer = new StringTokenizer(text, whitespaceChars);
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            word = setting.convertWord(word);
            if (word != null) {
                set.add(word);
            }
        }
    }

    /**
     * Create the trigger.
     *
     * @param conn the database connection
     * @param schema the schema name
     * @param table the table name
     */
    private static void createTrigger(Connection conn, String schema,
            String table) throws SQLException {
        createOrDropTrigger(conn, schema, table, true);
    }

    private static void createOrDropTrigger(Connection conn,
            String schema, String table, boolean create) throws SQLException {
        try (Statement stat = conn.createStatement()) {
            String trigger = StringUtils.quoteIdentifier(schema) + "."
                    + StringUtils.quoteIdentifier(TRIGGER_PREFIX + table);
            stat.execute("DROP TRIGGER IF EXISTS " + trigger);
            if (create) {
                boolean multiThread = FullTextTrigger.isMultiThread(conn);
                StringBuilder buff = new StringBuilder(
                        "CREATE TRIGGER IF NOT EXISTS ");
                // unless multithread, trigger needs to be called on rollback as well,
                // because we use the init connection do to changes in the index
                // (not the user connection)
                buff.append(trigger).
                        append(" AFTER INSERT, UPDATE, DELETE");
                if(!multiThread) {
                    buff.append(", ROLLBACK");
                }
                buff.append(" ON ").
                        append(StringUtils.quoteIdentifier(schema)).
                        append('.').
                        append(StringUtils.quoteIdentifier(table)).
                        append(" FOR EACH ROW CALL \"").
                        append(FullText.FullTextTrigger.class.getName()).
                        append('\"');
                stat.execute(buff.toString());
            }
        }
    }

    /**
     * Add the existing data to the index.
     *
     * @param conn the database connection
     * @param schema the schema name
     * @param table the table name
     */
    private static void indexExistingRows(Connection conn, String schema,
            String table) throws SQLException {
        FullText.FullTextTrigger existing = new FullText.FullTextTrigger();
        existing.init(conn, schema, null, table, false, Trigger.INSERT);
        String sql = "SELECT * FROM " + StringUtils.quoteIdentifier(schema)
                + "." + StringUtils.quoteIdentifier(table);
        ResultSet rs = conn.createStatement().executeQuery(sql);
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            Object[] row = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getObject(i + 1);
            }
            existing.fire(conn, null, row);
        }
    }

    private static String quoteString(String data) {
        if (data.indexOf('\'') < 0) {
            return "'" + data + "'";
        }
        int len = data.length();
        StringBuilder buff = new StringBuilder(len + 2);
        buff.append('\'');
        for (int i = 0; i < len; i++) {
            char ch = data.charAt(i);
            if (ch == '\'') {
                buff.append(ch);
            }
            buff.append(ch);
        }
        buff.append('\'');
        return buff.toString();
    }

    private static void setIgnoreList(FullTextSettings setting,
            String commaSeparatedList) {
        String[] list = StringUtils.arraySplit(commaSeparatedList, ',', true);
        setting.addIgnored(Arrays.asList(list));
    }

    /**
     * Check if a the indexed columns of a row probably have changed. It may
     * return true even if the change was minimal (for example from 0.0 to
     * 0.00).
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @param indexColumns the indexed columns
     * @return true if the indexed columns don't match
     */
    protected static boolean hasChanged(Object[] oldRow, Object[] newRow,
            int[] indexColumns) {
        for (int c : indexColumns) {
            Object o = oldRow[c], n = newRow[c];
            if (o == null) {
                if (n != null) {
                    return true;
                }
            } else if (!o.equals(n)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Trigger updates the index when a inserting, updating, or deleting a row.
     */
    public static final class FullTextTrigger implements Trigger {
        private FullTextSettings          setting;
        private IndexInfo                 index;
        private int[]                     columnTypes;
        private final PreparedStatement[] prepStatements = new PreparedStatement[SQL.length];
        private boolean                   useOwnConnection;

        private static final int INSERT_WORD = 0;
        private static final int INSERT_ROW  = 1;
        private static final int INSERT_MAP  = 2;
        private static final int DELETE_ROW  = 3;
        private static final int DELETE_MAP  = 4;
        private static final int SELECT_ROW  = 5;

        private static final String SQL[] = {
            "MERGE INTO " + SCHEMA + ".WORDS(NAME) KEY(NAME) VALUES(?)",
            "INSERT INTO " + SCHEMA + ".ROWS(HASH, INDEXID, KEY) VALUES(?, ?, ?)",
            "INSERT INTO " + SCHEMA + ".MAP(ROWID, WORDID) VALUES(?, ?)",
            "DELETE FROM " + SCHEMA + ".ROWS WHERE HASH=? AND INDEXID=? AND KEY=?",
            "DELETE FROM " + SCHEMA + ".MAP WHERE ROWID=? AND WORDID=?",
            "SELECT ID FROM " + SCHEMA + ".ROWS WHERE HASH=? AND INDEXID=? AND KEY=?"
        };

        /**
         * INTERNAL
         */
        @Override
        public void init(Connection conn, String schemaName, String triggerName,
                String tableName, boolean before, int type) throws SQLException {
            setting = FullTextSettings.getInstance(conn);
            if (!setting.isInitialized()) {
                FullText.init(conn);
            }
            ArrayList<String> keyList = New.arrayList();
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getColumns(null,
                    StringUtils.escapeMetaDataPattern(schemaName),
                    StringUtils.escapeMetaDataPattern(tableName),
                    null);
            ArrayList<String> columnList = New.arrayList();
            while (rs.next()) {
                columnList.add(rs.getString("COLUMN_NAME"));
            }
            columnTypes = new int[columnList.size()];
            index = new IndexInfo();
            index.schema = schemaName;
            index.table = tableName;
            index.columns = columnList.toArray(new String[0]);
            rs = meta.getColumns(null,
                    StringUtils.escapeMetaDataPattern(schemaName),
                    StringUtils.escapeMetaDataPattern(tableName),
                    null);
            for (int i = 0; rs.next(); i++) {
                columnTypes[i] = rs.getInt("DATA_TYPE");
            }
            if (keyList.isEmpty()) {
                rs = meta.getPrimaryKeys(null,
                        StringUtils.escapeMetaDataPattern(schemaName),
                        tableName);
                while (rs.next()) {
                    keyList.add(rs.getString("COLUMN_NAME"));
                }
            }
            if (keyList.isEmpty()) {
                throw throwException("No primary key for table " + tableName);
            }
            ArrayList<String> indexList = New.arrayList();
            PreparedStatement prep = conn.prepareStatement(
                    "SELECT ID, COLUMNS FROM " + SCHEMA + ".INDEXES" +
                    " WHERE SCHEMA=? AND TABLE=?");
            prep.setString(1, schemaName);
            prep.setString(2, tableName);
            rs = prep.executeQuery();
            if (rs.next()) {
                index.id = rs.getInt(1);
                String columns = rs.getString(2);
                if (columns != null) {
                    Collections.addAll(indexList, StringUtils.arraySplit(columns, ',', true));
                }
            }
            if (indexList.isEmpty()) {
                indexList.addAll(columnList);
            }
            index.keys = new int[keyList.size()];
            setColumns(index.keys, keyList, columnList);
            index.indexColumns = new int[indexList.size()];
            setColumns(index.indexColumns, indexList, columnList);
            setting.addIndexInfo(index);

            useOwnConnection = isMultiThread(conn);
            if(!useOwnConnection) {
                for (int i = 0; i < SQL.length; i++) {
                    prepStatements[i] = conn.prepareStatement(SQL[i],
                            Statement.RETURN_GENERATED_KEYS);
                }
            }
        }

        /**
         * Check whether the database is in multi-threaded mode.
         *
         * @param conn the connection
         * @return true if the multi-threaded mode is used
         */
        static boolean isMultiThread(Connection conn)
                throws SQLException {
            try (Statement stat = conn.createStatement()) {
                ResultSet rs = stat.executeQuery(
                                "SELECT value FROM information_schema.settings" +
                                " WHERE name = 'MULTI_THREADED'");
                return rs.next() && !"0".equals(rs.getString(1));
            }
        }

        /**
         * INTERNAL
         */
        @Override
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            if (oldRow != null) {
                if (newRow != null) {
                    // update
                    if (hasChanged(oldRow, newRow, index.indexColumns)) {
                        delete(conn, oldRow);
                        insert(conn, newRow);
                    }
                } else {
                    // delete
                    delete(conn, oldRow);
                }
            } else if (newRow != null) {
                // insert
                insert(conn, newRow);
            }
        }

        /**
         * INTERNAL
         */
        @Override
        public void close() {
            setting.removeIndexInfo(index);
        }

        /**
         * INTERNAL
         */
        @Override
        public void remove() {
            setting.removeIndexInfo(index);
        }

        /**
         * Add a row to the index.
         *
         * @param conn to use
         * @param row the row
         */
        protected void insert(Connection conn, Object[] row) throws SQLException {
            PreparedStatement prepInsertRow = null;
            PreparedStatement prepInsertMap = null;
            try {
                String key = getKey(row);
                int hash = key.hashCode();
                prepInsertRow = getStatement(conn, INSERT_ROW);
                prepInsertRow.setInt(1, hash);
                prepInsertRow.setInt(2, index.id);
                prepInsertRow.setString(3, key);
                prepInsertRow.execute();
                ResultSet rs = prepInsertRow.getGeneratedKeys();
                rs.next();
                int rowId = rs.getInt(1);

                prepInsertMap = getStatement(conn, INSERT_MAP);
                prepInsertMap.setInt(1, rowId);
                int[] wordIds = getWordIds(conn, row);
                for (int id : wordIds) {
                    prepInsertMap.setInt(2, id);
                    prepInsertMap.execute();
                }
            } finally {
                if (useOwnConnection) {
                    IOUtils.closeSilently(prepInsertRow);
                    IOUtils.closeSilently(prepInsertMap);
                }
            }
        }

        /**
         * Delete a row from the index.
         *
         * @param conn to use
         * @param row the row
         */
        protected void delete(Connection conn, Object[] row) throws SQLException {
            PreparedStatement prepSelectRow = null;
            PreparedStatement prepDeleteMap = null;
            PreparedStatement prepDeleteRow = null;
            try {
                String key = getKey(row);
                int hash = key.hashCode();
                prepSelectRow = getStatement(conn, SELECT_ROW);
                prepSelectRow.setInt(1, hash);
                prepSelectRow.setInt(2, index.id);
                prepSelectRow.setString(3, key);
                ResultSet rs = prepSelectRow.executeQuery();
                prepDeleteMap = getStatement(conn, DELETE_MAP);
                prepDeleteRow = getStatement(conn, DELETE_ROW);
                if (rs.next()) {
                    int rowId = rs.getInt(1);
                    prepDeleteMap.setInt(1, rowId);
                    int[] wordIds = getWordIds(conn, row);
                    for (int id : wordIds) {
                        prepDeleteMap.setInt(2, id);
                        prepDeleteMap.executeUpdate();
                    }
                    prepDeleteRow.setInt(1, hash);
                    prepDeleteRow.setInt(2, index.id);
                    prepDeleteRow.setString(3, key);
                    prepDeleteRow.executeUpdate();
                }
            } finally {
                if (useOwnConnection) {
                    IOUtils.closeSilently(prepSelectRow);
                    IOUtils.closeSilently(prepDeleteMap);
                    IOUtils.closeSilently(prepDeleteRow);
                }
            }
        }

        private int[] getWordIds(Connection conn, Object[] row) throws SQLException {
            HashSet<String> words = new HashSet<>();
            for (int idx : index.indexColumns) {
                int type = columnTypes[idx];
                Object data = row[idx];
                if (type == Types.CLOB && data != null) {
                    Reader reader;
                    if (data instanceof Reader) {
                        reader = (Reader) data;
                    } else {
                        reader = ((Clob) data).getCharacterStream();
                    }
                    addWords(setting, words, reader);
                } else {
                    String string = asString(data, type);
                    addWords(setting, words, string);
                }
            }
            PreparedStatement prepInsertWord = null;
            try {
                prepInsertWord = getStatement(conn, INSERT_WORD);
                int[] wordIds = new int[words.size()];
                int i = 0;
                for (String word : words) {
                    int wordId;
                    Integer wId;
                    while((wId = setting.getWordId(word)) == null) {
                        prepInsertWord.setString(1, word);
                        prepInsertWord.execute();
                        ResultSet rs = prepInsertWord.getGeneratedKeys();
                        if (rs.next()) {
                            wordId = rs.getInt(1);
                            if (wordId != 0) {
                                setting.addWord(word, wordId);
                                wId = wordId;
                                break;
                            }
                        }
                    }
                    wordIds[i++] = wId;
                }
                Arrays.sort(wordIds);
                return wordIds;
            } finally {
                if (useOwnConnection) {
                    IOUtils.closeSilently(prepInsertWord);
                }
            }
        }

        private String getKey(Object[] row) throws SQLException {
            StatementBuilder buff = new StatementBuilder();
            for (int columnIndex : index.keys) {
                buff.appendExceptFirst(" AND ");
                buff.append(StringUtils.quoteIdentifier(index.columns[columnIndex]));
                Object o = row[columnIndex];
                if (o == null) {
                    buff.append(" IS NULL");
                } else {
                    buff.append('=').append(quoteSQL(o, columnTypes[columnIndex]));
                }
            }
            return buff.toString();
        }

        private PreparedStatement getStatement(Connection conn, int index) throws SQLException {
            return useOwnConnection ?
                    conn.prepareStatement(SQL[index], Statement.RETURN_GENERATED_KEYS)
                    : prepStatements[index];
        }

    }

    /**
     * INTERNAL
     * Close all fulltext settings, freeing up memory.
     */
    public static void closeAll() {
        FullTextSettings.closeAll();
    }

    /**
     * Throw a SQLException with the given message.
     *
     * @param message the message
     * @return never returns normally
     * @throws SQLException the exception
     */
    protected static SQLException throwException(String message)
            throws SQLException {
        throw new SQLException(message, "FULLTEXT");
    }
}
