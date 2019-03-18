/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.fulltext;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.h2.api.Trigger;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.expression.ExpressionColumn;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.fs.FileUtils;
import org.h2.tools.SimpleResultSet;
import org.h2.util.New;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * This class implements the full text search based on Apache Lucene.
 * Most methods can be called using SQL statements as well.
 */
public class FullTextLucene extends FullText {

    /**
     * Whether the text content should be stored in the Lucene index.
     */
    protected static final boolean STORE_DOCUMENT_TEXT_IN_INDEX =
            Utils.getProperty("h2.storeDocumentTextInIndex", false);

    private static final HashMap<String, IndexAccess> INDEX_ACCESS = new HashMap<>();
    private static final String TRIGGER_PREFIX = "FTL_";
    private static final String SCHEMA = "FTL";
    private static final String LUCENE_FIELD_DATA = "_DATA";
    private static final String LUCENE_FIELD_QUERY = "_QUERY";
    private static final String LUCENE_FIELD_MODIFIED = "_modified";
    private static final String LUCENE_FIELD_COLUMN_PREFIX = "_";

    /**
     * The prefix for a in-memory path. This prefix is only used internally
     * within this class and not related to the database URL.
     */
    private static final String IN_MEMORY_PREFIX = "mem:";

    /**
     * Initializes full text search functionality for this database. This adds
     * the following Java functions to the database:
     * <ul>
     * <li>FTL_CREATE_INDEX(schemaNameString, tableNameString,
     * columnListString)</li>
     * <li>FTL_SEARCH(queryString, limitInt, offsetInt): result set</li>
     * <li>FTL_REINDEX()</li>
     * <li>FTL_DROP_ALL()</li>
     * </ul>
     * It also adds a schema FTL to the database where bookkeeping information
     * is stored. This function may be called from a Java application, or by
     * using the SQL statements:
     *
     * <pre>
     * CREATE ALIAS IF NOT EXISTS FTL_INIT FOR
     *      &quot;org.h2.fulltext.FullTextLucene.init&quot;;
     * CALL FTL_INIT();
     * </pre>
     *
     * @param conn the connection
     */
    public static void init(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement()) {
            stat.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            stat.execute("CREATE TABLE IF NOT EXISTS " + SCHEMA +
                    ".INDEXES(SCHEMA VARCHAR, TABLE VARCHAR, " +
                    "COLUMNS VARCHAR, PRIMARY KEY(SCHEMA, TABLE))");
            stat.execute("CREATE ALIAS IF NOT EXISTS FTL_CREATE_INDEX FOR \"" +
                    FullTextLucene.class.getName() + ".createIndex\"");
            stat.execute("CREATE ALIAS IF NOT EXISTS FTL_DROP_INDEX FOR \"" +
                    FullTextLucene.class.getName() + ".dropIndex\"");
            stat.execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH FOR \"" +
                    FullTextLucene.class.getName() + ".search\"");
            stat.execute("CREATE ALIAS IF NOT EXISTS FTL_SEARCH_DATA FOR \"" +
                    FullTextLucene.class.getName() + ".searchData\"");
            stat.execute("CREATE ALIAS IF NOT EXISTS FTL_REINDEX FOR \"" +
                    FullTextLucene.class.getName() + ".reindex\"");
            stat.execute("CREATE ALIAS IF NOT EXISTS FTL_DROP_ALL FOR \"" +
                    FullTextLucene.class.getName() + ".dropAll\"");
        }
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

        PreparedStatement prep = conn.prepareStatement("DELETE FROM " + SCHEMA
                + ".INDEXES WHERE SCHEMA=? AND TABLE=?");
        prep.setString(1, schema);
        prep.setString(2, table);
        int rowCount = prep.executeUpdate();
        if (rowCount != 0) {
            reindex(conn);
        }
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
        removeIndexFiles(conn);
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM " + SCHEMA + ".INDEXES");
        while (rs.next()) {
            String schema = rs.getString("SCHEMA");
            String table = rs.getString("TABLE");
            createTrigger(conn, schema, table);
            indexExistingRows(conn, schema, table);
        }
    }

    /**
     * Drops all full text indexes from the database.
     *
     * @param conn the connection
     */
    public static void dropAll(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE");
        removeAllTriggers(conn, TRIGGER_PREFIX);
        removeIndexFiles(conn);
    }

    /**
     * Searches from the full text index for this database.
     * The returned result set has the following column:
     * <ul><li>QUERY (varchar): the query to use to get the data.
     * The query does not include 'SELECT * FROM '. Example:
     * PUBLIC.TEST WHERE ID = 1
     * </li><li>SCORE (float) the relevance score as returned by Lucene.
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
        return search(conn, text, limit, offset, false);
    }

    /**
     * Searches from the full text index for this database. The result contains
     * the primary key data as an array. The returned result set has the
     * following columns:
     * <ul>
     * <li>SCHEMA (varchar): the schema name. Example: PUBLIC</li>
     * <li>TABLE (varchar): the table name. Example: TEST</li>
     * <li>COLUMNS (array of varchar): comma separated list of quoted column
     * names. The column names are quoted if necessary. Example: (ID)</li>
     * <li>KEYS (array of values): comma separated list of values.
     * Example: (1)</li>
     * <li>SCORE (float) the relevance score as returned by Lucene.</li>
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
        return search(conn, text, limit, offset, true);
    }

    /**
     * Convert an exception to a fulltext exception.
     *
     * @param e the original exception
     * @return the converted SQL exception
     */
    protected static SQLException convertException(Exception e) {
        return new SQLException("Error while indexing document", "FULLTEXT", e);
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
        Statement stat = conn.createStatement();
        String trigger = StringUtils.quoteIdentifier(schema) + "." +
                StringUtils.quoteIdentifier(TRIGGER_PREFIX + table);
        stat.execute("DROP TRIGGER IF EXISTS " + trigger);
        if (create) {
            StringBuilder buff = new StringBuilder(
                    "CREATE TRIGGER IF NOT EXISTS ");
            // the trigger is also called on rollback because transaction
            // rollback will not undo the changes in the Lucene index
            buff.append(trigger).
                append(" AFTER INSERT, UPDATE, DELETE, ROLLBACK ON ").
                append(StringUtils.quoteIdentifier(schema)).
                append('.').
                append(StringUtils.quoteIdentifier(table)).
                append(" FOR EACH ROW CALL \"").
                append(FullTextLucene.FullTextTrigger.class.getName()).
                append('\"');
            stat.execute(buff.toString());
        }
    }

    /**
     * Get the index writer/searcher wrapper for the given connection.
     *
     * @param conn the connection
     * @return the index access wrapper
     */
    protected static IndexAccess getIndexAccess(Connection conn)
            throws SQLException {
        String path = getIndexPath(conn);
        synchronized (INDEX_ACCESS) {
            IndexAccess access = INDEX_ACCESS.get(path);
            if (access == null) {
                try {
                    Directory indexDir = path.startsWith(IN_MEMORY_PREFIX) ?
                            new RAMDirectory() : FSDirectory.open(new File(path));
                    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
                    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_30, analyzer);
                    conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
                    IndexWriter writer = new IndexWriter(indexDir, conf);
                    //see http://wiki.apache.org/lucene-java/NearRealtimeSearch
                    access = new IndexAccess(writer);
                } catch (IOException e) {
                    throw convertException(e);
                }
                INDEX_ACCESS.put(path, access);
            }
            return access;
        }
    }

    /**
     * Get the path of the Lucene index for this database.
     *
     * @param conn the database connection
     * @return the path
     */
    protected static String getIndexPath(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("CALL DATABASE_PATH()");
        rs.next();
        String path = rs.getString(1);
        if (path == null) {
            return IN_MEMORY_PREFIX + conn.getCatalog();
        }
        int index = path.lastIndexOf(':');
        // position 1 means a windows drive letter is used, ignore that
        if (index > 1) {
            path = path.substring(index + 1);
        }
        rs.close();
        return path;
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
        FullTextLucene.FullTextTrigger existing = new FullTextLucene.FullTextTrigger();
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
            existing.insert(row, false);
        }
        existing.commitIndex();
    }

    private static void removeIndexFiles(Connection conn) throws SQLException {
        String path = getIndexPath(conn);
        removeIndexAccess(path);
        if (!path.startsWith(IN_MEMORY_PREFIX)) {
            FileUtils.deleteRecursive(path, false);
        }
    }

    /**
     * Close the index writer and searcher and remove them from the index access
     * set.
     *
     * @param indexPath the index path
     */
    protected static void removeIndexAccess(String indexPath)
            throws SQLException {
        synchronized (INDEX_ACCESS) {
            try {
                IndexAccess access = INDEX_ACCESS.remove(indexPath);
                if(access != null) {
                    access.close();
                }
            } catch (Exception e) {
                throw convertException(e);
            }
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
    protected static ResultSet search(Connection conn, String text,
            int limit, int offset, boolean data) throws SQLException {
        SimpleResultSet result = createResultSet(data);
        if (conn.getMetaData().getURL().startsWith("jdbc:columnlist:")) {
            // this is just to query the result set columns
            return result;
        }
        if (text == null || text.trim().length() == 0) {
            return result;
        }
        try {
            IndexAccess access = getIndexAccess(conn);
            // take a reference as the searcher may change
            IndexSearcher searcher = access.getSearcher();
            try {
                // reuse the same analyzer; it's thread-safe;
                // also allows subclasses to control the analyzer used.
                Analyzer analyzer = access.writer.getAnalyzer();
                QueryParser parser = new QueryParser(Version.LUCENE_30,
                        LUCENE_FIELD_DATA, analyzer);
                Query query = parser.parse(text);
                // Lucene 3 insists on a hard limit and will not provide
                // a total hits value. Take at least 100 which is
                // an optimal limit for Lucene as any more
                // will trigger writing results to disk.
                int maxResults = (limit == 0 ? 100 : limit) + offset;
                TopDocs docs = searcher.search(query, maxResults);
                if (limit == 0) {
                    limit = docs.totalHits;
                }
                for (int i = 0, len = docs.scoreDocs.length; i < limit
                        && i + offset < docs.totalHits
                        && i + offset < len; i++) {
                    ScoreDoc sd = docs.scoreDocs[i + offset];
                    Document doc = searcher.doc(sd.doc);
                    float score = sd.score;
                    String q = doc.get(LUCENE_FIELD_QUERY);
                    if (data) {
                        int idx = q.indexOf(" WHERE ");
                        JdbcConnection c = (JdbcConnection) conn;
                        Session session = (Session) c.getSession();
                        Parser p = new Parser(session);
                        String tab = q.substring(0, idx);
                        ExpressionColumn expr = (ExpressionColumn) p
                                .parseExpression(tab);
                        String schemaName = expr.getOriginalTableAliasName();
                        String tableName = expr.getColumnName();
                        q = q.substring(idx + " WHERE ".length());
                        Object[][] columnData = parseKey(conn, q);
                        result.addRow(schemaName, tableName, columnData[0],
                                columnData[1], score);
                    } else {
                        result.addRow(q, score);
                    }
                }
            } finally {
                access.returnSearcher(searcher);
            }
        } catch (Exception e) {
            throw convertException(e);
        }
        return result;
    }

    /**
     * Trigger updates the index when a inserting, updating, or deleting a row.
     */
    public static final class FullTextTrigger implements Trigger {

        private String schema;
        private String table;
        private int[] keys;
        private int[] indexColumns;
        private String[] columns;
        private int[] columnTypes;
        private String indexPath;
        private IndexAccess indexAccess;

        /**
         * INTERNAL
         */
        @Override
        public void init(Connection conn, String schemaName, String triggerName,
                String tableName, boolean before, int type) throws SQLException {
            this.schema = schemaName;
            this.table = tableName;
            this.indexPath = getIndexPath(conn);
            this.indexAccess = getIndexAccess(conn);
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
            columns = columnList.toArray(new String[0]);
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
                    "SELECT COLUMNS FROM " + SCHEMA
                    + ".INDEXES WHERE SCHEMA=? AND TABLE=?");
            prep.setString(1, schemaName);
            prep.setString(2, tableName);
            rs = prep.executeQuery();
            if (rs.next()) {
                String cols = rs.getString(1);
                if (cols != null) {
                    Collections.addAll(indexList,
                            StringUtils.arraySplit(cols, ',', true));
                }
            }
            if (indexList.isEmpty()) {
                indexList.addAll(columnList);
            }
            keys = new int[keyList.size()];
            setColumns(keys, keyList, columnList);
            indexColumns = new int[indexList.size()];
            setColumns(indexColumns, indexList, columnList);
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
                    if (hasChanged(oldRow, newRow, indexColumns)) {
                        delete(oldRow, false);
                        insert(newRow, true);
                    }
                } else {
                    // delete
                    delete(oldRow, true);
                }
            } else if (newRow != null) {
                // insert
                insert(newRow, true);
            }
        }

        /**
         * INTERNAL
         */
        @Override
        public void close() throws SQLException {
            removeIndexAccess(indexPath);
        }

        /**
         * INTERNAL
         */
        @Override
        public void remove() {
            // ignore
        }

        /**
         * Commit all changes to the Lucene index.
         */
        void commitIndex() throws SQLException {
            try {
                indexAccess.commit();
            } catch (IOException e) {
                throw convertException(e);
            }
        }

        /**
         * Add a row to the index.
         *
         * @param row the row
         * @param commitIndex whether to commit the changes to the Lucene index
         */
        protected void insert(Object[] row, boolean commitIndex) throws SQLException {
            String query = getQuery(row);
            Document doc = new Document();
            doc.add(new Field(LUCENE_FIELD_QUERY, query,
                    Field.Store.YES, Field.Index.NOT_ANALYZED));
            long time = System.currentTimeMillis();
            doc.add(new Field(LUCENE_FIELD_MODIFIED,
                    DateTools.timeToString(time, DateTools.Resolution.SECOND),
                    Field.Store.YES, Field.Index.NOT_ANALYZED));
            StatementBuilder buff = new StatementBuilder();
            for (int index : indexColumns) {
                String columnName = columns[index];
                String data = asString(row[index], columnTypes[index]);
                // column names that start with _
                // must be escaped to avoid conflicts
                // with internal field names (_DATA, _QUERY, _modified)
                if (columnName.startsWith(LUCENE_FIELD_COLUMN_PREFIX)) {
                    columnName = LUCENE_FIELD_COLUMN_PREFIX + columnName;
                }
                doc.add(new Field(columnName, data,
                        Field.Store.NO, Field.Index.ANALYZED));
                buff.appendExceptFirst(" ");
                buff.append(data);
            }
            Field.Store storeText = STORE_DOCUMENT_TEXT_IN_INDEX ?
                    Field.Store.YES : Field.Store.NO;
            doc.add(new Field(LUCENE_FIELD_DATA, buff.toString(), storeText,
                    Field.Index.ANALYZED));
            try {
                indexAccess.writer.addDocument(doc);
                if (commitIndex) {
                    commitIndex();
                }
            } catch (IOException e) {
                throw convertException(e);
            }
        }

        /**
         * Delete a row from the index.
         *
         * @param row the row
         * @param commitIndex whether to commit the changes to the Lucene index
         */
        protected void delete(Object[] row, boolean commitIndex) throws SQLException {
            String query = getQuery(row);
            try {
                Term term = new Term(LUCENE_FIELD_QUERY, query);
                indexAccess.writer.deleteDocuments(term);
                if (commitIndex) {
                    commitIndex();
                }
            } catch (IOException e) {
                throw convertException(e);
            }
        }

        private String getQuery(Object[] row) throws SQLException {
            StatementBuilder buff = new StatementBuilder();
            if (schema != null) {
                buff.append(StringUtils.quoteIdentifier(schema)).append('.');
            }
            buff.append(StringUtils.quoteIdentifier(table)).append(" WHERE ");
            for (int columnIndex : keys) {
                buff.appendExceptFirst(" AND ");
                buff.append(StringUtils.quoteIdentifier(columns[columnIndex]));
                Object o = row[columnIndex];
                if (o == null) {
                    buff.append(" IS NULL");
                } else {
                    buff.append('=').append(FullText.quoteSQL(o, columnTypes[columnIndex]));
                }
            }
            return buff.toString();
        }
    }

    /**
     * A wrapper for the Lucene writer and searcher.
     */
    static final class IndexAccess {

        /**
         * The index writer.
         */
        final IndexWriter writer;

        /**
         * Map of usage counters for outstanding searchers.
         */
        private final Map<IndexSearcher,Integer> counters = new HashMap<>();

        /**
         * Usage counter for current searcher.
         */
        private int counter;

        /**
         * The index searcher.
         */
        private IndexSearcher searcher;

        IndexAccess(IndexWriter writer) throws IOException {
            this.writer = writer;
            IndexReader reader = IndexReader.open(writer, true);
            searcher = new IndexSearcher(reader);
        }

        /**
         * Start using the searcher.
         *
         * @return the searcher
         */
        synchronized IndexSearcher getSearcher() {
            ++counter;
            return searcher;
        }

        /**
         * Stop using the searcher.
         *
         * @param searcher the searcher
         */
        synchronized void returnSearcher(IndexSearcher searcher) {
            if (this.searcher == searcher) {
                --counter;
                assert counter >= 0;
            } else {
                Integer cnt = counters.remove(searcher);
                assert cnt != null;
                if(--cnt == 0) {
                    closeSearcher(searcher);
                } else {
                    counters.put(searcher, cnt);
                }
            }
        }

        /**
         * Commit the changes.
         */
        public synchronized void commit() throws IOException {
            writer.commit();
            if (counter != 0) {
                counters.put(searcher, counter);
                counter = 0;
            } else {
                closeSearcher(searcher);
            }
            // recreate Searcher with the IndexWriter's reader.
            searcher = new IndexSearcher(IndexReader.open(writer, true));
        }

        /**
         * Close the index.
         */
        public synchronized void close() throws IOException {
            for (IndexSearcher searcher : counters.keySet()) {
                closeSearcher(searcher);
            }
            counters.clear();
            closeSearcher(searcher);
            searcher = null;
            writer.close();
        }

        private static void closeSearcher(IndexSearcher searcher) {
            IndexReader indexReader = searcher.getIndexReader();
            try { searcher.close(); } catch(IOException ignore) {/**/}
            try { indexReader.close(); } catch(IOException ignore) {/**/}
        }
    }
}
