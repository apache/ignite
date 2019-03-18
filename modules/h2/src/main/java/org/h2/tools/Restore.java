/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.Tool;

/**
 * Restores a H2 database by extracting the database files from a .zip file.
 * @h2.resource
 */
public class Restore extends Tool {

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-file &lt;filename&gt;]</td>
     * <td>The source file name (default: backup.zip)</td></tr>
     * <tr><td>[-dir &lt;dir&gt;]</td>
     * <td>The target directory (default: .)</td></tr>
     * <tr><td>[-db &lt;database&gt;]</td>
     * <td>The target database name (as stored if not set)</td></tr>
     * <tr><td>[-quiet]</td>
     * <td>Do not print progress information</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Restore().runTool(args);
    }

    @Override
    public void runTool(String... args) throws SQLException {
        String zipFileName = "backup.zip";
        String dir = ".";
        String db = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-dir")) {
                dir = args[++i];
            } else if (arg.equals("-file")) {
                zipFileName = args[++i];
            } else if (arg.equals("-db")) {
                db = args[++i];
            } else if (arg.equals("-quiet")) {
                // ignore
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        execute(zipFileName, dir, db);
    }

    private static String getOriginalDbName(String fileName, String db)
            throws IOException {

        try (InputStream in = FileUtils.newInputStream(fileName)) {
            ZipInputStream zipIn = new ZipInputStream(in);
            String originalDbName = null;
            boolean multiple = false;
            while (true) {
                ZipEntry entry = zipIn.getNextEntry();
                if (entry == null) {
                    break;
                }
                String entryName = entry.getName();
                zipIn.closeEntry();
                String name = getDatabaseNameFromFileName(entryName);
                if (name != null) {
                    if (db.equals(name)) {
                        originalDbName = name;
                        // we found the correct database
                        break;
                    } else if (originalDbName == null) {
                        originalDbName = name;
                        // we found a database, but maybe another one
                    } else {
                        // we have found multiple databases, but not the correct
                        // one
                        multiple = true;
                    }
                }
            }
            zipIn.close();
            if (multiple && !db.equals(originalDbName)) {
                throw new IOException("Multiple databases found, but not " + db);
            }
            return originalDbName;
        }
    }

    /**
     * Extract the name of the database from a given file name.
     * Only files ending with .h2.db are considered, all others return null.
     *
     * @param fileName the file name (without directory)
     * @return the database name or null
     */
    private static String getDatabaseNameFromFileName(String fileName) {
        if (fileName.endsWith(Constants.SUFFIX_PAGE_FILE)) {
            return fileName.substring(0,
                    fileName.length() - Constants.SUFFIX_PAGE_FILE.length());
        }
        if (fileName.endsWith(Constants.SUFFIX_MV_FILE)) {
            return fileName.substring(0,
                    fileName.length() - Constants.SUFFIX_MV_FILE.length());
        }
        return null;
    }

    /**
     * Restores database files.
     *
     * @param zipFileName the name of the backup file
     * @param directory the directory name
     * @param db the database name (null for all databases)
     * @throws DbException if there is an IOException
     */
    public static void execute(String zipFileName, String directory, String db) {
        InputStream in = null;
        try {
            if (!FileUtils.exists(zipFileName)) {
                throw new IOException("File not found: " + zipFileName);
            }
            String originalDbName = null;
            int originalDbLen = 0;
            if (db != null) {
                originalDbName = getOriginalDbName(zipFileName, db);
                if (originalDbName == null) {
                    throw new IOException("No database named " + db + " found");
                }
                if (originalDbName.startsWith(SysProperties.FILE_SEPARATOR)) {
                    originalDbName = originalDbName.substring(1);
                }
                originalDbLen = originalDbName.length();
            }
            in = FileUtils.newInputStream(zipFileName);
            try (ZipInputStream zipIn = new ZipInputStream(in)) {
                while (true) {
                    ZipEntry entry = zipIn.getNextEntry();
                    if (entry == null) {
                        break;
                    }
                    String fileName = entry.getName();
                    // restoring windows backups on linux and vice versa
                    fileName = fileName.replace('\\', SysProperties.FILE_SEPARATOR.charAt(0));
                    fileName = fileName.replace('/', SysProperties.FILE_SEPARATOR.charAt(0));
                    if (fileName.startsWith(SysProperties.FILE_SEPARATOR)) {
                        fileName = fileName.substring(1);
                    }
                    boolean copy = false;
                    if (db == null) {
                        copy = true;
                    } else if (fileName.startsWith(originalDbName + ".")) {
                        fileName = db + fileName.substring(originalDbLen);
                        copy = true;
                    }
                    if (copy) {
                        OutputStream o = null;
                        try {
                            o = FileUtils.newOutputStream(
                                    directory + SysProperties.FILE_SEPARATOR + fileName, false);
                            IOUtils.copy(zipIn, o);
                            o.close();
                        } finally {
                            IOUtils.closeSilently(o);
                        }
                    }
                    zipIn.closeEntry();
                }
                zipIn.closeEntry();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, zipFileName);
        } finally {
            IOUtils.closeSilently(in);
        }
    }

}
