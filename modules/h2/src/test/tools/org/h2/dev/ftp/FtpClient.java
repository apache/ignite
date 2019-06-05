/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.ftp;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.h2.util.IOUtils;
import org.h2.util.NetUtils;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * A simple standalone FTP client.
 */
public class FtpClient {
    private Socket socket;
    private BufferedReader reader;
    private PrintWriter writer;
    private int code;
    private String message;
    private InputStream inData;
    private OutputStream outData;

    private FtpClient() {
        // don't allow construction
    }

    /**
     * Open an FTP connection.
     *
     * @param url the FTP URL
     * @return the ftp client object
     */
    public static FtpClient open(String url) throws IOException {
        FtpClient client = new FtpClient();
        client.connect(url);
        return client;
    }

    private void connect(String url) throws IOException {
        socket = NetUtils.createSocket(url, 21, false);
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        reader = new BufferedReader(new InputStreamReader(in));
        writer = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        readCode(220);
    }

    private void readLine() throws IOException {
        while (true) {
            message = reader.readLine();
            if (message != null) {
                int idxSpace = message.indexOf(' ');
                int idxMinus = message.indexOf('-');
                int idx = idxSpace < 0 ? idxMinus : idxMinus < 0 ? idxSpace
                        : Math.min(idxSpace, idxMinus);
                if (idx < 0) {
                    code = 0;
                } else {
                    code = Integer.parseInt(message.substring(0, idx));
                    message = message.substring(idx + 1);
                }
            }
            break;
        }
    }

    private void readCode(int optional, int expected) throws IOException {
        readLine();
        if (code == optional) {
            readLine();
        }
        if (code != expected) {
            throw new IOException("Expected: " + expected + " got: " + code
                    + " " + message);
        }
    }

    private void readCode(int expected) throws IOException {
        readCode(-1, expected);
    }

    private void send(String command) {
        writer.println(command);
        writer.flush();
    }

    /**
     * Login to this FTP server (USER, PASS, SYST, SITE, STRU F, TYPE I).
     *
     * @param userName the user name
     * @param password the password
     */
    public void login(String userName, String password) throws IOException {
        send("USER " + userName);
        readCode(331);
        send("PASS " + password);
        readCode(230);
        send("SYST");
        readCode(215);
        send("SITE");
        readCode(500);
        send("STRU F");
        readCode(200);
        send("TYPE I");
        readCode(200);
    }

    /**
     * Close the connection (QUIT).
     */
    public void close() throws IOException {
        if (socket != null) {
            send("QUIT");
            readCode(221);
            socket.close();
        }
    }

    /**
     * Change the working directory (CWD).
     *
     * @param dir the new directory
     */
    public void changeWorkingDirectory(String dir) throws IOException {
        send("CWD " + dir);
        readCode(250);
    }

    /**
     * Change to the parent directory (CDUP).
     */
    public void changeDirectoryUp() throws IOException {
        send("CDUP");
        readCode(250);
    }

    /**
     * Delete a file (DELE).
     *
     * @param fileName the name of the file to delete
     */
    void delete(String fileName) throws IOException {
        send("DELE " + fileName);
        readCode(226, 250);
    }

    /**
     * Create a directory (MKD).
     *
     * @param dir the directory to create
     */
    public void makeDirectory(String dir) throws IOException {
        send("MKD " + dir);
        readCode(226, 257);
    }

    /**
     * Change the transfer mode (MODE).
     *
     * @param mode the mode
     */
    void mode(String mode) throws IOException {
        send("MODE " + mode);
        readCode(200);
    }

    /**
     * Change the modified time of a file (MDTM).
     *
     * @param fileName the file name
     */
    void modificationTime(String fileName) throws IOException {
        send("MDTM " + fileName);
        readCode(213);
    }

    /**
     * Issue a no-operation statement (NOOP).
     */
    void noOperation() throws IOException {
        send("NOOP");
        readCode(200);
    }

    /**
     * Print the working directory (PWD).
     *
     * @return the working directory
     */
    String printWorkingDirectory() throws IOException {
        send("PWD");
        readCode(257);
        return removeQuotes();
    }

    private String removeQuotes() {
        int first = message.indexOf('"') + 1;
        int last = message.lastIndexOf('"');
        StringBuilder buff = new StringBuilder();
        for (int i = first; i < last; i++) {
            char ch = message.charAt(i);
            buff.append(ch);
            if (ch == '\"') {
                i++;
            }
        }
        return buff.toString();
    }

    private void passive() throws IOException {
        send("PASV");
        readCode(226, 227);
        int first = message.indexOf('(') + 1;
        int last = message.indexOf(')');
        String[] address = StringUtils.arraySplit(
                message.substring(first, last), ',', true);
        StatementBuilder buff = new StatementBuilder();
        for (int i = 0; i < 4; i++) {
            buff.appendExceptFirst(".");
            buff.append(address[i]);
        }
        String ip = buff.toString();
        InetAddress addr = InetAddress.getByName(ip);
        int port = (Integer.parseInt(address[4]) << 8) | Integer.parseInt(address[5]);
        Socket socketData = NetUtils.createSocket(addr, port, false);
        inData = socketData.getInputStream();
        outData = socketData.getOutputStream();
    }

    /**
     * Rename a file (RNFR / RNTO).
     *
     * @param fromFileName the old file name
     * @param toFileName the new file name
     */
    void rename(String fromFileName, String toFileName) throws IOException {
        send("RNFR " + fromFileName);
        readCode(350);
        send("RNTO " + toFileName);
        readCode(250);
    }

    /**
     * Read a file.
     *
     * @param fileName the file name
     * @return the content, null if the file doesn't exist
     */
    public byte[] retrieve(String fileName) throws IOException {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        retrieve(fileName, buff, 0);
        return buff.toByteArray();
    }

    /**
     * Read a file ([REST] RETR).
     *
     * @param fileName the file name
     * @param out the output stream
     * @param restartAt restart at the given position (0 if no restart is
     *            required).
     */
    void retrieve(String fileName, OutputStream out, long restartAt)
            throws IOException {
        passive();
        if (restartAt > 0) {
            send("REST " + restartAt);
            readCode(350);
        }
        send("RETR " + fileName);
        IOUtils.copyAndClose(inData, out);
        readCode(150, 226);
    }

    /**
     * Remove a directory (RMD).
     *
     * @param dir the directory to remove
     */
    public void removeDirectory(String dir) throws IOException {
        send("RMD " + dir);
        readCode(226, 250);
    }

    /**
     * Remove all files and directory in a directory, and then delete the
     * directory itself.
     *
     * @param dir the directory to remove
     */
    public void removeDirectoryRecursive(String dir) throws IOException {
        for (File f : listFiles(dir)) {
            String name = f.getName();
            if (f.isDirectory()) {
                if (!name.equals(".") && !name.equals("..")) {
                    removeDirectoryRecursive(dir + "/" + name);
                }
            } else {
                delete(dir + "/" + name);
            }
        }
        removeDirectory(dir);
    }

    /**
     * Get the size of a file (SIZE).
     *
     * @param fileName the file name
     * @return the size
     */
    long size(String fileName) throws IOException {
        send("SIZE " + fileName);
        readCode(250);
        long size = Long.parseLong(message);
        return size;
    }

    /**
     * Store a file (STOR).
     *
     * @param fileName the file name
     * @param in the input stream
     */
    public void store(String fileName, InputStream in) throws IOException {
        passive();
        send("STOR " + fileName);
        readCode(150);
        IOUtils.copyAndClose(in, outData);
        readCode(226);
    }

    /**
     * Copy a local file or directory to the FTP server, recursively.
     *
     * @param file the file to copy
     */
    public void storeRecursive(File file) throws IOException {
        if (file.isDirectory()) {
            makeDirectory(file.getName());
            changeWorkingDirectory(file.getName());
            for (File f : file.listFiles()) {
                storeRecursive(f);
            }
            changeWorkingDirectory("..");
        } else {
            InputStream in = new FileInputStream(file);
            store(file.getName(), in);
        }
    }

    /**
     * Get the directory listing (NLST).
     *
     * @param dir the directory
     * @return the listing
     */
    public String nameList(String dir) throws IOException {
        passive();
        send("NLST " + dir);
        readCode(150);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyAndClose(inData, out);
        readCode(226);
        byte[] data = out.toByteArray();
        return new String(data);
    }

    /**
     * Get the directory listing (LIST).
     *
     * @param dir the directory
     * @return the listing
     */
    public String list(String dir) throws IOException {
        passive();
        send("LIST " + dir);
        readCode(150);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyAndClose(inData, out);
        readCode(226);
        byte[] data = out.toByteArray();
        return new String(data);
    }

    /**
     * A file on an FTP server.
     */
    static class FtpFile extends File {
        private static final long serialVersionUID = 1L;
        private final boolean dir;
        private final long length;
        FtpFile(String name, boolean dir, long length) {
            super(name);
            this.dir = dir;
            this.length = length;
        }
        @Override
        public long length() {
            return length;
        }
        @Override
        public boolean isFile() {
            return !dir;
        }
        @Override
        public boolean isDirectory() {
            return dir;
        }
        @Override
        public boolean exists() {
            return true;
        }
    }

    /**
     * Check if a file exists on the FTP server.
     *
     * @param dir the directory
     * @param name the directory or file name
     * @return true if it exists
     */
    public boolean exists(String dir, String name) throws IOException  {
        for (File f : listFiles(dir)) {
            if (f.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * List the files on the FTP server.
     *
     * @param dir the directory
     * @return the list of files
     */
    public File[] listFiles(String dir) throws IOException {
        String content = list(dir);
        String[] list = StringUtils.arraySplit(content.trim(), '\n', true);
        File[] files = new File[list.length];
        for (int i = 0; i < files.length; i++) {
            String s = list[i];
            while (true) {
                String s2 = StringUtils.replaceAll(s, "  ", " ");
                if (s2.equals(s)) {
                    break;
                }
                s = s2;
            }
            String[] tokens = StringUtils.arraySplit(s, ' ', true);
            boolean directory = tokens[0].charAt(0) == 'd';
            long length = Long.parseLong(tokens[4]);
            String name = tokens[8];
            File f = new FtpFile(name, directory, length);
            files[i] = f;
        }
        return files;
    }

}
