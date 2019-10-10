/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.test.TestBase;
import org.h2.tools.Server;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * Represents a database in the benchmark test application.
 */
class Database {

    private static final boolean TRACE = true;

    private DatabaseTest test;
    private int id;
    private String name, url, user, password;
    private final ArrayList<String[]> replace = new ArrayList<>();
    private String currentAction;
    private long startTimeNs;
    private long initialGCTime;
    private Connection conn;
    private Statement stat;
    private long lastTrace;
    private final Random random = new Random(1);
    private final ArrayList<Object[]> results = new ArrayList<>();
    private int totalTime;
    private int totalGCTime;
    private final AtomicInteger executedStatements = new AtomicInteger(0);
    private int threadCount;

    private Server serverH2;
    private Object serverDerby;
    private boolean serverHSQLDB;

    /**
     * Get the database name.
     *
     * @return the database name
     */
    String getName() {
        return name;
    }

    /**
     * Get the total measured time.
     *
     * @return the time
     */
    int getTotalTime() {
        return totalTime;
    }

    /**
     * Get the total measured GC time.
     *
     * @return the time in milliseconds
     */
    int getTotalGCTime() {
        return totalGCTime;
    }

    /**
     * Get the result array.
     *
     * @return the result array
     */
    ArrayList<Object[]> getResults() {
        return results;
    }

    /**
     * Get the random number generator.
     *
     * @return the generator
     */
    Random getRandom() {
        return random;
    }

    /**
     * Start the server if the this is a remote connection.
     */
    void startServer() throws Exception {
        if (url.startsWith("jdbc:h2:tcp:")) {
            serverH2 = Server.createTcpServer().start();
            Thread.sleep(100);
        } else if (url.startsWith("jdbc:derby://")) {
            serverDerby = Class.forName(
                    "org.apache.derby.drda.NetworkServerControl").newInstance();
            Method m = serverDerby.getClass().getMethod("start", PrintWriter.class);
            m.invoke(serverDerby, new Object[] { null });
            // serverDerby = new NetworkServerControl();
            // serverDerby.start(null);
            Thread.sleep(100);
        } else if (url.startsWith("jdbc:hsqldb:hsql:")) {
            if (!serverHSQLDB) {
                Class<?> c;
                try {
                    c = Class.forName("org.hsqldb.server.Server");
                } catch (Exception e) {
                    c = Class.forName("org.hsqldb.Server");
                }
                Method m = c.getMethod("main", String[].class);
                m.invoke(null, new Object[] { new String[] { "-database.0",
                        "data/mydb;hsqldb.default_table_type=cached", "-dbname.0", "xdb" } });
                // org.hsqldb.Server.main(new String[]{"-database.0", "mydb",
                // "-dbname.0", "xdb"});
                serverHSQLDB = true;
                Thread.sleep(100);
            }
        }
    }

    /**
     * Stop the server if this is a remote connection.
     */
    void stopServer() throws Exception {
        if (serverH2 != null) {
            serverH2.stop();
            serverH2 = null;
        }
        if (serverDerby != null) {
            Method m = serverDerby.getClass().getMethod("shutdown");
            // cast for JDK 1.5
            m.invoke(serverDerby, (Object[]) null);
            // serverDerby.shutdown();
            serverDerby = null;
        } else if (serverHSQLDB) {
            // can not shut down (shutdown calls System.exit)
            // openConnection();
            // update("SHUTDOWN");
            // closeConnection();
            // serverHSQLDB = false;
        }
    }

    /**
     * Parse a database configuration and create a database object from it.
     *
     * @param test the test application
     * @param id the database id
     * @param dbString the configuration string
     * @param threadCount the number of threads to use
     * @return a new database object with the given settings
     */
    static Database parse(DatabaseTest test, int id, String dbString,
            int threadCount) {
        try {
            StringTokenizer tokenizer = new StringTokenizer(dbString, ",");
            Database db = new Database();
            db.id = id;
            db.threadCount = threadCount;
            db.test = test;
            db.name = tokenizer.nextToken().trim();
            String driver = tokenizer.nextToken().trim();
            Class.forName(driver);
            db.url = tokenizer.nextToken().trim();
            db.user = tokenizer.nextToken().trim();
            db.password = "";
            if (tokenizer.hasMoreTokens()) {
                db.password = tokenizer.nextToken().trim();
            }
            return db;
        } catch (Exception e) {
            System.out.println("Cannot load database " + dbString + " :" + e.toString());
            return null;
        }
    }

    /**
     * Open a new database connection. This connection must be closed
     * by calling conn.close().
     *
     * @return the opened connection
     */
    Connection openNewConnection() throws SQLException {
        Connection newConn = DriverManager.getConnection(url, user, password);
        if (url.startsWith("jdbc:derby:")) {
            // Derby: use higher cache size
            try (Statement s = newConn.createStatement()) {
                // stat.execute("CALL
                // SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(
                // 'derby.storage.pageCacheSize', '64')");
                // stat.execute("CALL
                // SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(
                // 'derby.storage.pageSize', '8192')");
            }
        } else if (url.startsWith("jdbc:hsqldb:")) {
            // HSQLDB: use a WRITE_DELAY of 1 second
            try (Statement s = newConn.createStatement()) {
                s.execute("SET WRITE_DELAY 1");
            }
        }
        return newConn;
    }

    /**
     * Open the database connection.
     */
    void openConnection() throws SQLException {
        conn = openNewConnection();
        stat = conn.createStatement();
    }

    /**
     * Close the database connection.
     */
    void closeConnection() throws SQLException {
        // if(!serverHSQLDB && url.startsWith("jdbc:hsqldb:")) {
        //     stat.execute("SHUTDOWN");
        // }
        conn.close();
        stat = null;
        conn = null;
    }

    /**
     * Initialize the SQL statement translation of this database.
     *
     * @param prop the properties with the translations to use
     */
    void setTranslations(Properties prop) {
        String databaseType = url.substring("jdbc:".length());
        databaseType = databaseType.substring(0, databaseType.indexOf(':'));
        for (Object k : prop.keySet()) {
            String key = (String) k;
            if (key.startsWith(databaseType + ".")) {
                String pattern = key.substring(databaseType.length() + 1);
                pattern = pattern.replace('_', ' ');
                pattern = StringUtils.toUpperEnglish(pattern);
                String replacement = prop.getProperty(key);
                replace.add(new String[]{pattern, replacement});
            }
        }
    }

    /**
     * Prepare a SQL statement.
     *
     * @param sql the SQL statement
     * @return the prepared statement
     */
    PreparedStatement prepare(String sql) throws SQLException {
        sql = getSQL(sql);
        return conn.prepareStatement(sql);
    }

    private String getSQL(String sql) {
        for (String[] pair : replace) {
            String pattern = pair[0];
            String replacement = pair[1];
            sql = StringUtils.replaceAll(sql, pattern, replacement);
        }
        return sql;
    }

    /**
     * Start the benchmark.
     *
     * @param bench the benchmark
     * @param action the action
     */
    void start(Bench bench, String action) {
        this.currentAction = bench.getName() + ": " + action;
        this.startTimeNs = System.nanoTime();
        this.initialGCTime = Utils.getGarbageCollectionTime();
    }

    /**
     * This method is called when the test run ends. This will stop collecting
     * data.
     */
    void end() {
        long time = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs);
        long gcCollectionTime = Utils.getGarbageCollectionTime() - initialGCTime;
        log(currentAction, "ms", (int) time);
        if (test.isCollect()) {
            totalTime += time;
            totalGCTime += gcCollectionTime;
        }
    }

    /**
     * Drop a table. Errors are ignored.
     *
     * @param table the table name
     */
    void dropTable(String table) {
        try {
            update("DROP TABLE " + table);
        } catch (Exception e) {
            // ignore - table may not exist
        }
    }

    /**
     * Execute an SQL statement.
     *
     * @param prep the prepared statement
     * @param traceMessage the trace message
     */
    void update(PreparedStatement prep, String traceMessage) throws SQLException {
        test.trace(traceMessage);
        prep.executeUpdate();
        if (test.isCollect()) {
            executedStatements.incrementAndGet();
        }
    }

    /**
     * Execute an SQL statement.
     *
     * @param sql the SQL statement
     */
    void update(String sql) throws SQLException {
        sql = getSQL(sql);
        if (sql.trim().length() > 0) {
            if (test.isCollect()) {
                executedStatements.incrementAndGet();
            }
            stat.execute(sql);
        } else {
            System.out.println("?");
        }
    }

    /**
     * Enable or disable auto-commit.
     *
     * @param b false to disable
     */
    void setAutoCommit(boolean b) throws SQLException {
        conn.setAutoCommit(b);
    }

    /**
     * Commit a transaction.
     */
    void commit() throws SQLException {
        conn.commit();
    }

    /**
     * Roll a transaction back.
     */
    void rollback() throws SQLException {
        conn.rollback();
    }

    /**
     * Print trace information if trace is enabled.
     *
     * @param action the action
     * @param i the current value
     * @param max the maximum value
     */
    void trace(String action, int i, int max) {
        if (TRACE) {
            long time = System.nanoTime();
            if (i == 0 || lastTrace == 0) {
                lastTrace = time;
            } else if (time > lastTrace + TimeUnit.SECONDS.toNanos(1)) {
                System.out.println(action + ": " + ((100 * i / max) + "%"));
                lastTrace = time;
            }
        }
    }

    /**
     * If data collection is enabled, add the currently used memory size to the
     * log.
     *
     * @param bench the benchmark
     * @param action the action
     */
    void logMemory(Bench bench, String action) {
        log(bench.getName() + ": " + action, "MB", TestBase.getMemoryUsed());
    }

    /**
     * If data collection is enabled, add this information to the log.
     *
     * @param action the action
     * @param scale the scale
     * @param value the value
     */
    void log(String action, String scale, int value) {
        if (test.isCollect()) {
            results.add(new Object[] { action, scale, Integer.valueOf(value) });
        }
    }

    /**
     * Execute a query.
     *
     * @param prep the prepared statement
     * @return the result set
     */
    ResultSet query(PreparedStatement prep) throws SQLException {
        // long time = System.nanoTime();
        ResultSet rs = prep.executeQuery();
        // time = System.nanoTime() - time;
        // if(time > 100) {
        //     System.out.println("time="+time);
        // }
        if (test.isCollect()) {
            executedStatements.incrementAndGet();
        }
        return rs;
    }

    /**
     * Execute a query and read all rows.
     *
     * @param prep the prepared statement
     */
    void queryReadResult(PreparedStatement prep) throws SQLException {
        ResultSet rs = query(prep);
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                rs.getString(i + 1);
            }
        }
    }

    /**
     * Get the number of executed statements.
     *
     * @return the number of statements
     */
    int getExecutedStatements() {
        return executedStatements.get();
    }

    /**
     * Get the database id.
     *
     * @return the id
     */
    int getId() {
        return id;
    }

    int getThreadsCount() {
        return threadCount;
    }

    /**
     * The interface used for a test.
     */
    public interface DatabaseTest {

        /**
         * Whether data needs to be collected.
         *
         * @return true if yes
         */
        boolean isCollect();

        /**
         * Print a message to system out if trace is enabled.
         *
         * @param msg the message
         */
        void trace(String msg);

    }

}
