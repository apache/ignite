/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.h2.test.utils.SelfDestructor;
import org.h2.tools.DeleteDbFiles;

/**
 * The base class for tests that use connections to database.
 */
public abstract class TestDb extends TestBase {

    /**
     * Start the TCP server if enabled in the configuration.
     */
    protected void startServerIfRequired() throws SQLException {
        config.beforeTest();
    }

    /**
     * Open a database connection in admin mode. The default user name and
     * password is used.
     *
     * @param name the database name
     * @return the connection
     */
    public Connection getConnection(String name) throws SQLException {
        return getConnectionInternal(getURL(name, true), getUser(),
                getPassword());
    }

    /**
     * Open a database connection.
     *
     * @param name the database name
     * @param user the user name to use
     * @param password the password to use
     * @return the connection
     */
    public Connection getConnection(String name, String user, String password)
            throws SQLException {
        return getConnectionInternal(getURL(name, false), user, password);
    }

    /**
     * Get the database URL for the given database name using the current
     * configuration options.
     *
     * @param name the database name
     * @param admin true if the current user is an admin
     * @return the database URL
     */
    protected String getURL(String name, boolean admin) {
        String url;
        if (name.startsWith("jdbc:")) {
            if (config.mvStore) {
                name = addOption(name, "MV_STORE", "true");
            } else {
                name = addOption(name, "MV_STORE", "false");
            }
            return name;
        }
        if (admin) {
            // name = addOption(name, "RETENTION_TIME", "10");
            // name = addOption(name, "WRITE_DELAY", "10");
        }
        int idx = name.indexOf(':');
        if (idx == -1 && config.memory) {
            name = "mem:" + name;
        } else {
            if (idx < 0 || idx > 10) {
                // index > 10 if in options
                name = getBaseDir() + "/" + name;
            }
        }
        if (config.networked) {
            if (config.ssl) {
                url = "ssl://localhost:"+config.getPort()+"/" + name;
            } else {
                url = "tcp://localhost:"+config.getPort()+"/" + name;
            }
        } else if (config.googleAppEngine) {
            url = "gae://" + name +
                    ";FILE_LOCK=NO;AUTO_SERVER=FALSE;DB_CLOSE_ON_EXIT=FALSE";
        } else {
            url = name;
        }
        if (config.mvStore) {
            url = addOption(url, "MV_STORE", "true");
            url = addOption(url, "MAX_COMPACT_TIME", "0"); // to speed up tests
        } else {
            url = addOption(url, "MV_STORE", "false");
        }
        if (!config.memory) {
            if (config.smallLog && admin) {
                url = addOption(url, "MAX_LOG_SIZE", "1");
            }
        }
        if (config.traceSystemOut) {
            url = addOption(url, "TRACE_LEVEL_SYSTEM_OUT", "2");
        }
        if (config.traceLevelFile > 0 && admin) {
            url = addOption(url, "TRACE_LEVEL_FILE", "" + config.traceLevelFile);
            url = addOption(url, "TRACE_MAX_FILE_SIZE", "8");
        }
        url = addOption(url, "LOG", "1");
        if (config.throttleDefault > 0) {
            url = addOption(url, "THROTTLE", "" + config.throttleDefault);
        } else if (config.throttle > 0) {
            url = addOption(url, "THROTTLE", "" + config.throttle);
        }
        url = addOption(url, "LOCK_TIMEOUT", "" + config.lockTimeout);
        if (config.diskUndo && admin) {
            url = addOption(url, "MAX_MEMORY_UNDO", "3");
        }
        if (config.big && admin) {
            // force operations to disk
            url = addOption(url, "MAX_OPERATION_MEMORY", "1");
        }
        url = addOption(url, "MULTI_THREADED", config.multiThreaded ? "TRUE" : "FALSE");
        if (config.lazy) {
            url = addOption(url, "LAZY_QUERY_EXECUTION", "1");
        }
        if (config.cacheType != null && admin) {
            url = addOption(url, "CACHE_TYPE", config.cacheType);
        }
        if (config.diskResult && admin) {
            url = addOption(url, "MAX_MEMORY_ROWS", "100");
            url = addOption(url, "CACHE_SIZE", "0");
        }
        if (config.cipher != null) {
            url = addOption(url, "CIPHER", config.cipher);
        }
        if (config.defrag) {
            url = addOption(url, "DEFRAG_ALWAYS", "TRUE");
        }
        if (config.collation != null) {
            url = addOption(url, "COLLATION", config.collation);
        }
        return "jdbc:h2:" + url;
    }

    private static String addOption(String url, String option, String value) {
        if (url.indexOf(";" + option + "=") < 0) {
            url += ";" + option + "=" + value;
        }
        return url;
    }

    private static Connection getConnectionInternal(String url, String user,
            String password) throws SQLException {
        org.h2.Driver.load();
        // url += ";DEFAULT_TABLE_TYPE=1";
        // Class.forName("org.hsqldb.jdbcDriver");
        // return DriverManager.getConnection("jdbc:hsqldb:" + name, "sa", "");
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Delete all database files for this database.
     *
     * @param name the database name
     */
    protected void deleteDb(String name) {
        deleteDb(getBaseDir(), name);
    }

    /**
     * Delete all database files for a database.
     *
     * @param dir the directory where the database files are located
     * @param name the database name
     */
    protected void deleteDb(String dir, String name) {
        DeleteDbFiles.execute(dir, name, true);
        // ArrayList<String> list;
        // list = FileLister.getDatabaseFiles(baseDir, name, true);
        // if (list.size() >  0) {
        //    System.out.println("Not deleted: " + list);
        // }
    }

    /**
     * Build a child process.
     *
     * @param name the name
     * @param childClass the class
     * @param jvmArgs the argument list
     * @return the process builder
     */
    public ProcessBuilder buildChild(String name, Class<? extends TestDb> childClass,
            String... jvmArgs) {
        List<String> args = new ArrayList<>(16);
        args.add(getJVM());
        Collections.addAll(args, jvmArgs);
        Collections.addAll(args, "-cp", getClassPath(),
                        SelfDestructor.getPropertyString(1),
                        childClass.getName(),
                        "-url", getURL(name, true),
                        "-user", getUser(),
                        "-password", getPassword());
        ProcessBuilder processBuilder = new ProcessBuilder()
//                            .redirectError(ProcessBuilder.Redirect.INHERIT)
                            .redirectErrorStream(true)
                            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                            .command(args);
        return processBuilder;
    }

    public abstract static class Child extends TestDb {
        private String url;
        private String user;
        private String password;

        public Child(String... args) {
            for (int i = 0; i < args.length; i++) {
                if ("-url".equals(args[i])) {
                    url = args[++i];
                } else if ("-user".equals(args[i])) {
                    user = args[++i];
                } else if ("-password".equals(args[i])) {
                    password = args[++i];
                }
                SelfDestructor.startCountdown(60);
            }
        }

        public Connection getConnection() throws SQLException {
            return getConnection(url, user, password);
        }
    }

}
