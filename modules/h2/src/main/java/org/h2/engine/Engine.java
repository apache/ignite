/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.Parser;
import org.h2.command.dml.SetTypes;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.store.FileLock;
import org.h2.store.FileLockMethod;
import org.h2.util.MathUtils;
import org.h2.util.ThreadDeadlockDetector;
import org.h2.util.Utils;

/**
 * The engine contains a map of all open databases.
 * It is also responsible for opening and creating new databases.
 * This is a singleton class.
 */
public class Engine implements SessionFactory {

    private static final Engine INSTANCE = new Engine();
    private static final Map<String, Database> DATABASES = new HashMap<>();

    private volatile long wrongPasswordDelay =
            SysProperties.DELAY_WRONG_PASSWORD_MIN;
    private boolean jmx;

    private Engine() {
        // use getInstance()
        if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
            ThreadDeadlockDetector.init();
        }
    }

    public static Engine getInstance() {
        return INSTANCE;
    }

    private Session openSession(ConnectionInfo ci, boolean ifExists,
            String cipher) {
        String name = ci.getName();
        Database database;
        ci.removeProperty("NO_UPGRADE", false);
        boolean openNew = ci.getProperty("OPEN_NEW", false);
        boolean opened = false;
        User user = null;
        synchronized (DATABASES) {
            if (openNew || ci.isUnnamedInMemory()) {
                database = null;
            } else {
                database = DATABASES.get(name);
            }
            if (database == null) {
                if (ifExists && !Database.exists(name)) {
                    throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_1, name);
                }
                database = new Database(ci, cipher);
                opened = true;
                if (database.getAllUsers().isEmpty()) {
                    // users is the last thing we add, so if no user is around,
                    // the database is new (or not initialized correctly)
                    user = new User(database, database.allocateObjectId(),
                            ci.getUserName(), false);
                    user.setAdmin(true);
                    user.setUserPasswordHash(ci.getUserPasswordHash());
                    database.setMasterUser(user);
                }
                if (!ci.isUnnamedInMemory()) {
                    DATABASES.put(name, database);
                }
            }
        }
        if (opened) {
            // start the thread when already synchronizing on the database
            // otherwise a deadlock can occur when the writer thread
            // opens a new database (as in recovery testing)
            database.opened();
        }
        if (database.isClosing()) {
            return null;
        }
        if (user == null) {
            if (database.validateFilePasswordHash(cipher, ci.getFilePasswordHash())) {
                user = database.findUser(ci.getUserName());
                if (user != null) {
                    if (!user.validateUserPasswordHash(ci.getUserPasswordHash())) {
                        user = null;
                    }
                }
            }
            if (opened && (user == null || !user.isAdmin())) {
                // reset - because the user is not an admin, and has no
                // right to listen to exceptions
                database.setEventListener(null);
            }
        }
        if (user == null) {
            DbException er = DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            database.getTrace(Trace.DATABASE).error(er, "wrong user or password; user: \"" +
                    ci.getUserName() + "\"");
            database.removeSession(null);
            throw er;
        }
        checkClustering(ci, database);
        Session session = database.createSession(user);
        if (session == null) {
            // concurrently closing
            return null;
        }
        if (ci.getProperty("JMX", false)) {
            try {
                Utils.callStaticMethod(
                        "org.h2.jmx.DatabaseInfo.registerMBean", ci, database);
            } catch (Exception e) {
                database.removeSession(session);
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
            jmx = true;
        }
        return session;
    }

    /**
     * Open a database connection with the given connection information.
     *
     * @param ci the connection information
     * @return the session
     */
    @Override
    public Session createSession(ConnectionInfo ci) {
        return INSTANCE.createSessionAndValidate(ci);
    }

    private Session createSessionAndValidate(ConnectionInfo ci) {
        try {
            ConnectionInfo backup = null;
            String lockMethodName = ci.getProperty("FILE_LOCK", null);
            FileLockMethod fileLockMethod = FileLock.getFileLockMethod(lockMethodName);
            if (fileLockMethod == FileLockMethod.SERIALIZED) {
                // In serialized mode, database instance sharing is not possible
                ci.setProperty("OPEN_NEW", "TRUE");
                try {
                    backup = ci.clone();
                } catch (CloneNotSupportedException e) {
                    throw DbException.convert(e);
                }
            }
            Session session = openSession(ci);
            validateUserAndPassword(true);
            if (backup != null) {
                session.setConnectionInfo(backup);
            }
            return session;
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }

    private synchronized Session openSession(ConnectionInfo ci) {
        boolean ifExists = ci.removeProperty("IFEXISTS", false);
        boolean ignoreUnknownSetting = ci.removeProperty(
                "IGNORE_UNKNOWN_SETTINGS", false);
        String cipher = ci.removeProperty("CIPHER", null);
        String init = ci.removeProperty("INIT", null);
        Session session;
        for (int i = 0;; i++) {
            session = openSession(ci, ifExists, cipher);
            if (session != null) {
                break;
            }
            // we found a database that is currently closing
            // wait a bit to avoid a busy loop (the method is synchronized)
            if (i > 60 * 1000) {
                // retry at most 1 minute
                throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1,
                        "Waited for database closing longer than 1 minute");
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        synchronized (session) {
            session.setAllowLiterals(true);
            DbSettings defaultSettings = DbSettings.getDefaultSettings();
            for (String setting : ci.getKeys()) {
                if (defaultSettings.containsKey(setting)) {
                    // database setting are only used when opening the database
                    continue;
                }
                String value = ci.getProperty(setting);
                try {
                    CommandInterface command = session.prepareCommand(
                            "SET " + Parser.quoteIdentifier(setting) + " " + value,
                            Integer.MAX_VALUE);
                    command.executeUpdate(false);
                } catch (DbException e) {
                    if (e.getErrorCode() == ErrorCode.ADMIN_RIGHTS_REQUIRED) {
                        session.getTrace().error(e, "admin rights required; user: \"" +
                                ci.getUserName() + "\"");
                    } else {
                        session.getTrace().error(e, "");
                    }
                    if (!ignoreUnknownSetting) {
                        session.close();
                        throw e;
                    }
                }
            }
            if (init != null) {
                try {
                    CommandInterface command = session.prepareCommand(init,
                            Integer.MAX_VALUE);
                    command.executeUpdate(false);
                } catch (DbException e) {
                    if (!ignoreUnknownSetting) {
                        session.close();
                        throw e;
                    }
                }
            }
            session.setAllowLiterals(false);
            session.commit(true);
        }
        return session;
    }

    private static void checkClustering(ConnectionInfo ci, Database database) {
        String clusterSession = ci.getProperty(SetTypes.CLUSTER, null);
        if (Constants.CLUSTERING_DISABLED.equals(clusterSession)) {
            // in this case, no checking is made
            // (so that a connection can be made to disable/change clustering)
            return;
        }
        String clusterDb = database.getCluster();
        if (!Constants.CLUSTERING_DISABLED.equals(clusterDb)) {
            if (!Constants.CLUSTERING_ENABLED.equals(clusterSession)) {
                if (!Objects.equals(clusterSession, clusterDb)) {
                    if (clusterDb.equals(Constants.CLUSTERING_DISABLED)) {
                        throw DbException.get(
                                ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_ALONE);
                    }
                    throw DbException.get(
                            ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1,
                            clusterDb);
                }
            }
        }
    }

    /**
     * Called after a database has been closed, to remove the object from the
     * list of open databases.
     *
     * @param name the database name
     */
    void close(String name) {
        if (jmx) {
            try {
                Utils.callStaticMethod("org.h2.jmx.DatabaseInfo.unregisterMBean", name);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
        }
        synchronized (DATABASES) {
            DATABASES.remove(name);
        }
    }

    /**
     * This method is called after validating user name and password. If user
     * name and password were correct, the sleep time is reset, otherwise this
     * method waits some time (to make brute force / rainbow table attacks
     * harder) and then throws a 'wrong user or password' exception. The delay
     * is a bit randomized to protect against timing attacks. Also the delay
     * doubles after each unsuccessful logins, to make brute force attacks
     * harder.
     *
     * There is only one exception message both for wrong user and for
     * wrong password, to make it harder to get the list of user names. This
     * method must only be called from one place, so it is not possible from the
     * stack trace to see if the user name was wrong or the password.
     *
     * @param correct if the user name or the password was correct
     * @throws DbException the exception 'wrong user or password'
     */
    private void validateUserAndPassword(boolean correct) {
        int min = SysProperties.DELAY_WRONG_PASSWORD_MIN;
        if (correct) {
            long delay = wrongPasswordDelay;
            if (delay > min && delay > 0) {
                // the first correct password must be blocked,
                // otherwise parallel attacks are possible
                synchronized (INSTANCE) {
                    // delay up to the last delay
                    // an attacker can't know how long it will be
                    delay = MathUtils.secureRandomInt((int) delay);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    wrongPasswordDelay = min;
                }
            }
        } else {
            // this method is not synchronized on the Engine, so that
            // regular successful attempts are not blocked
            synchronized (INSTANCE) {
                long delay = wrongPasswordDelay;
                int max = SysProperties.DELAY_WRONG_PASSWORD_MAX;
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                }
                wrongPasswordDelay += wrongPasswordDelay;
                if (wrongPasswordDelay > max || wrongPasswordDelay < 0) {
                    wrongPasswordDelay = max;
                }
                if (min > 0) {
                    // a bit more to protect against timing attacks
                    delay += Math.abs(MathUtils.secureRandomLong() % 100);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
        }
    }

}
