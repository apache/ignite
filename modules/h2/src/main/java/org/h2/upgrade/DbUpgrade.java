/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.upgrade;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * This class starts the conversion from older database versions to the current
 * version if the respective classes are found.
 */
public class DbUpgrade {

    private static final boolean UPGRADE_CLASSES_PRESENT;

    private static boolean scriptInTempDir;
    private static boolean deleteOldDb;

    static {
        UPGRADE_CLASSES_PRESENT = Utils.isClassPresent("org.h2.upgrade.v1_1.Driver");
    }

    /**
     * If the upgrade classes are present, upgrade the database, or connect
     * using the old version (if the parameter NO_UPGRADE is set to true). If
     * the database is upgraded, or if no upgrade is possible or needed, this
     * methods returns null.
     *
     * @param url the database URL
     * @param info the properties
     * @return the connection if connected with the old version (NO_UPGRADE)
     */
    public static Connection connectOrUpgrade(String url, Properties info)
            throws SQLException {
        if (!UPGRADE_CLASSES_PRESENT) {
            return null;
        }
        Properties i2 = new Properties();
        i2.putAll(info);
        // clone so that the password (if set as a char array) is not cleared
        Object o = info.get("password");
        if (o instanceof char[]) {
            i2.put("password", StringUtils.cloneCharArray((char[]) o));
        }
        info = i2;
        ConnectionInfo ci = new ConnectionInfo(url, info);
        if (ci.isRemote() || !ci.isPersistent()) {
            return null;
        }
        String name = ci.getName();
        if (FileUtils.exists(name + Constants.SUFFIX_PAGE_FILE)) {
            return null;
        }
        if (!FileUtils.exists(name + Constants.SUFFIX_OLD_DATABASE_FILE)) {
            return null;
        }
        if (ci.removeProperty("NO_UPGRADE", false)) {
            return connectWithOldVersion(url, info);
        }
        synchronized (DbUpgrade.class) {
            upgrade(ci, info);
            return null;
        }
    }

    /**
     * The conversion script file will per default be created in the db
     * directory. Use this method to change the directory to the temp
     * directory.
     *
     * @param scriptInTempDir true if the conversion script should be
     *        located in the temp directory.
     */
    public static void setScriptInTempDir(boolean scriptInTempDir) {
        DbUpgrade.scriptInTempDir = scriptInTempDir;
    }

    /**
     * Old files will be renamed to .backup after a successful conversion. To
     * delete them after the conversion, use this method with the parameter
     * 'true'.
     *
     * @param deleteOldDb if true, the old db files will be deleted.
     */
    public static void setDeleteOldDb(boolean deleteOldDb) {
        DbUpgrade.deleteOldDb = deleteOldDb;
    }

    private static Connection connectWithOldVersion(String url, Properties info)
            throws SQLException {
        url = "jdbc:h2v1_1:" + url.substring("jdbc:h2:".length()) +
                ";IGNORE_UNKNOWN_SETTINGS=TRUE";
        return DriverManager.getConnection(url, info);
    }

    private static void upgrade(ConnectionInfo ci, Properties info)
            throws SQLException {
        String name = ci.getName();
        String data = name + Constants.SUFFIX_OLD_DATABASE_FILE;
        String index = name + ".index.db";
        String lobs = name + ".lobs.db";
        String backupData = data + ".backup";
        String backupIndex = index + ".backup";
        String backupLobs = lobs + ".backup";
        String script = null;
        try {
            if (scriptInTempDir) {
                new File(Utils.getProperty("java.io.tmpdir", ".")).mkdirs();
                script = File.createTempFile(
                        "h2dbmigration", "backup.sql").getAbsolutePath();
            } else {
                script = name + ".script.sql";
            }
            String oldUrl = "jdbc:h2v1_1:" + name +
                    ";UNDO_LOG=0;LOG=0;LOCK_MODE=0";
            String cipher = ci.getProperty("CIPHER", null);
            if (cipher != null) {
                oldUrl += ";CIPHER=" + cipher;
            }
            Connection conn = DriverManager.getConnection(oldUrl, info);
            Statement stat = conn.createStatement();
            String uuid = UUID.randomUUID().toString();
            if (cipher != null) {
                stat.execute("script to '" + script +
                        "' cipher aes password '" + uuid + "' --hide--");
            } else {
                stat.execute("script to '" + script + "'");
            }
            conn.close();
            FileUtils.move(data, backupData);
            FileUtils.move(index, backupIndex);
            if (FileUtils.exists(lobs)) {
                FileUtils.move(lobs, backupLobs);
            }
            ci.removeProperty("IFEXISTS", false);
            conn = new JdbcConnection(ci, true);
            stat = conn.createStatement();
            if (cipher != null) {
                stat.execute("runscript from '" + script +
                        "' cipher aes password '" + uuid + "' --hide--");
            } else {
                stat.execute("runscript from '" + script + "'");
            }
            stat.execute("analyze");
            stat.execute("shutdown compact");
            stat.close();
            conn.close();
            if (deleteOldDb) {
                FileUtils.delete(backupData);
                FileUtils.delete(backupIndex);
                FileUtils.deleteRecursive(backupLobs, false);
            }
        } catch (Exception e)  {
            if (FileUtils.exists(backupData)) {
                FileUtils.move(backupData, data);
            }
            if (FileUtils.exists(backupIndex)) {
                FileUtils.move(backupIndex, index);
            }
            if (FileUtils.exists(backupLobs)) {
                FileUtils.move(backupLobs, lobs);
            }
            FileUtils.delete(name + ".h2.db");
            throw DbException.toSQLException(e);
        } finally {
            if (script != null) {
                FileUtils.delete(script);
            }
        }
    }

}
