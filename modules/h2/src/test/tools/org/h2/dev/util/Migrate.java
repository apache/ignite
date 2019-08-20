/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import org.h2.engine.Constants;
import org.h2.tools.RunScript;

/**
 * Migrate a H2 database version 1.1.x (page store not enabled) to 1.2.x (page
 * store format). This will download the H2 jar file version 1.2.127 from
 * maven.org if it doesn't exist, execute the Script tool (using Runtime.exec)
 * to create a backup.sql script, rename the old database file to *.backup,
 * created a new database (using the H2 jar file in the class path) using the
 * Script tool, and then delete the backup.sql file. Most utility methods are
 * copied from h2/src/tools/org/h2/build/BuildBase.java.
 */
public class Migrate {

    private static final String USER = "sa";
    private static final String PASSWORD  = "sa";
    private static final File OLD_H2_FILE = new File("./h2-1.2.127.jar");
    private static final String DOWNLOAD_URL =
            "http://repo2.maven.org/maven2/com/h2database/h2/1.2.127/h2-1.2.127.jar";
    private static final String CHECKSUM =
            "056e784c7cf009483366ab9cd8d21d02fe47031a";
    private static final String TEMP_SCRIPT = "backup.sql";
    private final PrintStream sysOut = System.out;
    private boolean quiet;

    /**
     * Migrate databases. The user name and password are both "sa".
     *
     * @param args the path (default is the current directory)
     * @throws Exception if conversion fails
     */
    public static void main(String... args) throws Exception {
        new Migrate().execute(new File(args.length == 1 ? args[0] : "."), true,
                USER, PASSWORD, false);
    }

    /**
     * Migrate a database.
     *
     * @param file the database file (must end with .data.db) or directory
     * @param recursive if the file parameter is in fact a directory (in which
     *            case the directory is scanned recursively)
     * @param user the user name of the database
     * @param password the password
     * @param runQuiet to run in quiet mode
     * @throws Exception if conversion fails
     */
    public void execute(File file, boolean recursive, String user,
            String password, boolean runQuiet) throws Exception {
        String pathToJavaExe = getJavaExecutablePath();
        this.quiet = runQuiet;
        if (file.isDirectory() && recursive) {
            for (File f : file.listFiles()) {
                execute(f, recursive, user, password, runQuiet);
            }
            return;
        }
        if (!file.getName().endsWith(Constants.SUFFIX_OLD_DATABASE_FILE)) {
            return;
        }
        println("Migrating " + file.getName());
        if (!OLD_H2_FILE.exists()) {
            download(OLD_H2_FILE.getAbsolutePath(), DOWNLOAD_URL, CHECKSUM);
        }
        String url = "jdbc:h2:" + file.getAbsolutePath();
        url = url.substring(0, url.length() - Constants.SUFFIX_OLD_DATABASE_FILE.length());
        exec(new String[] {
                pathToJavaExe,
                "-Xmx128m",
                "-cp", OLD_H2_FILE.getAbsolutePath(),
                "org.h2.tools.Script",
                "-script", TEMP_SCRIPT,
                "-url", url,
                "-user", user,
                "-password", password
        });
        file.renameTo(new File(file.getAbsoluteFile() + ".backup"));
        RunScript.execute(url, user, password, TEMP_SCRIPT, StandardCharsets.UTF_8, true);
        new File(TEMP_SCRIPT).delete();
    }

    private static String getJavaExecutablePath() {
        String pathToJava;
        if (File.separator.equals("\\")) {
            pathToJava = System.getProperty("java.home") + File.separator
                    + "bin" + File.separator + "java.exe";
        } else {
            pathToJava = System.getProperty("java.home") + File.separator
                    + "bin" + File.separator + "java";
        }
        if (!new File(pathToJava).exists()) {
            // Fallback to old behaviour
            pathToJava = "java";
        }
        return pathToJava;
    }

    private void download(String target, String fileURL, String sha1Checksum) {
        File targetFile = new File(target);
        if (targetFile.exists()) {
            return;
        }
        mkdirs(targetFile.getAbsoluteFile().getParentFile());
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        try {
            println("Downloading " + fileURL);
            URL url = new URL(fileURL);
            InputStream in = new BufferedInputStream(url.openStream());
            long last = System.nanoTime();
            int len = 0;
            while (true) {
                long now = System.nanoTime();
                if (now > last + TimeUnit.SECONDS.toNanos(1)) {
                    println("Downloaded " + len + " bytes");
                    last = now;
                }
                int x = in.read();
                len++;
                if (x < 0) {
                    break;
                }
                buff.write(x);
            }
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Error downloading", e);
        }
        byte[] data = buff.toByteArray();
        String got = getSHA1(data);
        if (sha1Checksum == null) {
            println("SHA1 checksum: " + got);
        } else {
            if (!got.equals(sha1Checksum)) {
                throw new RuntimeException("SHA1 checksum mismatch; got: " + got);
            }
        }
        writeFile(targetFile, data);
    }

    private static void mkdirs(File f) {
        if (!f.exists()) {
            if (!f.mkdirs()) {
                throw new RuntimeException("Can not create directory " + f.getAbsolutePath());
            }
        }
    }

    private void println(String s) {
        if (!quiet) {
            sysOut.println(s);
        }
    }

    private void print(String s) {
        if (!quiet) {
            sysOut.print(s);
        }
    }

    private static String getSHA1(byte[] data) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
            return convertBytesToString(md.digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static String convertBytesToString(byte[] value) {
        StringBuilder buff = new StringBuilder(value.length * 2);
        for (byte c : value) {
            int x = c & 0xff;
            buff.append(Integer.toString(x >> 4, 16)).
                append(Integer.toString(x & 0xf, 16));
        }
        return buff.toString();
    }

    private static void writeFile(File file, byte[] data) {
        try {
            RandomAccessFile ra = new RandomAccessFile(file, "rw");
            ra.write(data);
            ra.setLength(data.length);
            ra.close();
        } catch (IOException e) {
            throw new RuntimeException("Error writing to file " + file, e);
        }
    }

    private int exec(String[] command) {
        try {
            for (String c : command) {
                print(c + " ");
            }
            println("");
            Process p = Runtime.getRuntime().exec(command);
            copyInThread(p.getInputStream(), quiet ? null : sysOut);
            copyInThread(p.getErrorStream(), quiet ? null : sysOut);
            p.waitFor();
            return p.exitValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void copyInThread(final InputStream in, final OutputStream out) {
        new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        int x = in.read();
                        if (x < 0) {
                            return;
                        }
                        if (out != null) {
                            out.write(x);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
    }

}
