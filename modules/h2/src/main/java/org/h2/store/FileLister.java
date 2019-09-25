/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.nio.channels.FileChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.message.TraceSystem;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.util.New;

/**
 * Utility class to list the files of a database.
 */
public class FileLister {

    private FileLister() {
        // utility class
    }

    /**
     * Try to lock the database, and then unlock it. If this worked, the
     * .lock.db file will be removed.
     *
     * @param files the database files to check
     * @param message the text to include in the error message
     * @throws SQLException if it failed
     */
    public static void tryUnlockDatabase(List<String> files, String message)
            throws SQLException {
        for (String fileName : files) {
            if (fileName.endsWith(Constants.SUFFIX_LOCK_FILE)) {
                FileLock lock = new FileLock(new TraceSystem(null), fileName,
                        Constants.LOCK_SLEEP);
                try {
                    lock.lock(FileLockMethod.FILE);
                    lock.unlock();
                } catch (DbException e) {
                    throw DbException.get(
                            ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1,
                            message).getSQLException();
                }
            } else if (fileName.endsWith(Constants.SUFFIX_MV_FILE)) {
                try (FileChannel f = FilePath.get(fileName).open("r")) {
                    java.nio.channels.FileLock lock = f.tryLock(0, Long.MAX_VALUE, true);
                    lock.release();
                } catch (Exception e) {
                    throw DbException.get(
                            ErrorCode.CANNOT_CHANGE_SETTING_WHEN_OPEN_1, e,
                            message).getSQLException();
                }
            }
        }
    }

    /**
     * Normalize the directory name.
     *
     * @param dir the directory (null for the current directory)
     * @return the normalized directory name
     */
    public static String getDir(String dir) {
        if (dir == null || dir.equals("")) {
            return ".";
        }
        return FileUtils.toRealPath(dir);
    }

    /**
     * Get the list of database files.
     *
     * @param dir the directory (must be normalized)
     * @param db the database name (null for all databases)
     * @param all if true, files such as the lock, trace, and lob
     *            files are included. If false, only data, index, log,
     *            and lob files are returned
     * @return the list of files
     */
    public static ArrayList<String> getDatabaseFiles(String dir, String db,
            boolean all) {
        ArrayList<String> files = New.arrayList();
        // for Windows, File.getCanonicalPath("...b.") returns just "...b"
        String start = db == null ? null : (FileUtils.toRealPath(dir + "/" + db) + ".");
        for (String f : FileUtils.newDirectoryStream(dir)) {
            boolean ok = false;
            if (f.endsWith(Constants.SUFFIX_LOBS_DIRECTORY)) {
                if (start == null || f.startsWith(start)) {
                    files.addAll(getDatabaseFiles(f, null, all));
                    ok = true;
                }
            } else if (f.endsWith(Constants.SUFFIX_LOB_FILE)) {
                ok = true;
            } else if (f.endsWith(Constants.SUFFIX_PAGE_FILE)) {
                ok = true;
            } else if (f.endsWith(Constants.SUFFIX_MV_FILE)) {
                ok = true;
            } else if (all) {
                if (f.endsWith(Constants.SUFFIX_LOCK_FILE)) {
                    ok = true;
                } else if (f.endsWith(Constants.SUFFIX_TEMP_FILE)) {
                    ok = true;
                } else if (f.endsWith(Constants.SUFFIX_TRACE_FILE)) {
                    ok = true;
                }
            }
            if (ok) {
                if (db == null || f.startsWith(start)) {
                    files.add(f);
                }
            }
        }
        return files;
    }

}
