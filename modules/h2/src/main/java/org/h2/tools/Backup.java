/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.h2.command.dml.BackupCommand;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.store.FileLister;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.Tool;

/**
 * Creates a backup of a database.
 * <br />
 * This tool copies all database files. The database must be closed before using
 * this tool. To create a backup while the database is in use, run the BACKUP
 * SQL statement. In an emergency, for example if the application is not
 * responding, creating a backup using the Backup tool is possible by using the
 * quiet mode. However, if the database is changed while the backup is running
 * in quiet mode, the backup could be corrupt.
 *
 * @h2.resource
 */
public class Backup extends Tool {

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-file &lt;filename&gt;]</td>
     * <td>The target file name (default: backup.zip)</td></tr>
     * <tr><td>[-dir &lt;dir&gt;]</td>
     * <td>The source directory (default: .)</td></tr>
     * <tr><td>[-db &lt;database&gt;]</td>
     * <td>Source database; not required if there is only one</td></tr>
     * <tr><td>[-quiet]</td>
     * <td>Do not print progress information</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Backup().runTool(args);
    }

    @Override
    public void runTool(String... args) throws SQLException {
        String zipFileName = "backup.zip";
        String dir = ".";
        String db = null;
        boolean quiet = false;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-dir")) {
                dir = args[++i];
            } else if (arg.equals("-db")) {
                db = args[++i];
            } else if (arg.equals("-quiet")) {
                quiet = true;
            } else if (arg.equals("-file")) {
                zipFileName = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        try {
            process(zipFileName, dir, db, quiet);
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * Backs up database files.
     *
     * @param zipFileName the name of the target backup file (including path)
     * @param directory the source directory name
     * @param db the source database name (null if there is only one database,
     *            and and empty string to backup all files in this directory)
     * @param quiet don't print progress information
     */
    public static void execute(String zipFileName, String directory, String db,
            boolean quiet) throws SQLException {
        try {
            new Backup().process(zipFileName, directory, db, quiet);
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }

    private void process(String zipFileName, String directory, String db,
            boolean quiet) throws SQLException {
        List<String> list;
        boolean allFiles = db != null && db.length() == 0;
        if (allFiles) {
            list = FileUtils.newDirectoryStream(directory);
        } else {
            list = FileLister.getDatabaseFiles(directory, db, true);
        }
        if (list.isEmpty()) {
            if (!quiet) {
                printNoDatabaseFilesFound(directory, db);
            }
            return;
        }
        if (!quiet) {
            FileLister.tryUnlockDatabase(list, "backup");
        }
        zipFileName = FileUtils.toRealPath(zipFileName);
        FileUtils.delete(zipFileName);
        OutputStream fileOut = null;
        try {
            fileOut = FileUtils.newOutputStream(zipFileName, false);
            try (ZipOutputStream zipOut = new ZipOutputStream(fileOut)) {
                String base = "";
                for (String fileName : list) {
                    if (allFiles ||
                            fileName.endsWith(Constants.SUFFIX_PAGE_FILE) ||
                            fileName.endsWith(Constants.SUFFIX_MV_FILE)) {
                        base = FileUtils.getParent(fileName);
                        break;
                    }
                }
                for (String fileName : list) {
                    String f = FileUtils.toRealPath(fileName);
                    if (!f.startsWith(base)) {
                        DbException.throwInternalError(f + " does not start with " + base);
                    }
                    if (f.endsWith(zipFileName)) {
                        continue;
                    }
                    if (FileUtils.isDirectory(fileName)) {
                        continue;
                    }
                    f = f.substring(base.length());
                    f = BackupCommand.correctFileName(f);
                    ZipEntry entry = new ZipEntry(f);
                    zipOut.putNextEntry(entry);
                    InputStream in = null;
                    try {
                        in = FileUtils.newInputStream(fileName);
                        IOUtils.copyAndCloseInput(in, zipOut);
                    } catch (FileNotFoundException e) {
                        // the file could have been deleted in the meantime
                        // ignore this (in this case an empty file is created)
                    } finally {
                        IOUtils.closeSilently(in);
                    }
                    zipOut.closeEntry();
                    if (!quiet) {
                        out.println("Processed: " + fileName);
                    }
                }
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, zipFileName);
        } finally {
            IOUtils.closeSilently(fileOut);
        }
    }

}
