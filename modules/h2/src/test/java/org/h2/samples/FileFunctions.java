/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * This sample application shows how to create a user defined function
 * to read a file from the file system.
 */
public class FileFunctions {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS READ_TEXT_FILE " +
                "FOR \"org.h2.samples.FileFunctions.readTextFile\" ");
        stat.execute("CREATE ALIAS READ_TEXT_FILE_WITH_ENCODING " +
                "FOR \"org.h2.samples.FileFunctions.readTextFileWithEncoding\" ");
        stat.execute("CREATE ALIAS READ_FILE " +
                "FOR \"org.h2.samples.FileFunctions.readFile\" ");
        ResultSet rs = stat.executeQuery("CALL READ_FILE('test.txt')");
        rs.next();
        byte[] data = rs.getBytes(1);
        System.out.println("length: " + data.length);
        rs = stat.executeQuery("CALL READ_TEXT_FILE('test.txt')");
        rs.next();
        String text = rs.getString(1);
        System.out.println("text: " + text);
        stat.close();
        conn.close();
    }

    /**
     * Read a String from a file. The default encoding for this platform is
     * used.
     *
     * @param fileName the file name
     * @return the text
     */
    public static String readTextFile(String fileName) throws IOException {
        byte[] buff = readFile(fileName);
        String s = new String(buff);
        return s;
    }

    /**
     * Read a String from a file using the specified encoding.
     *
     * @param fileName the file name
     * @param encoding the encoding
     * @return the text
     */
    public static String readTextFileWithEncoding(String fileName,
            String encoding) throws IOException {
        byte[] buff = readFile(fileName);
        String s = new String(buff, encoding);
        return s;
    }

    /**
     * Read a file into a byte array.
     *
     * @param fileName the file name
     * @return the byte array
     */
    public static byte[] readFile(String fileName) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
            byte[] buff = new byte[(int) file.length()];
            file.readFully(buff);
            return buff;
        }
    }
}
