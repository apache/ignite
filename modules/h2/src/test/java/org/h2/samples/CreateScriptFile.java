/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import org.h2.engine.Constants;
import org.h2.security.SHA256;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.FileStoreOutputStream;
import org.h2.store.fs.FileUtils;
import org.h2.tools.CompressTool;
import org.h2.tools.RunScript;
import org.h2.tools.Script;

/**
 * This sample application shows how to manually
 * create an encrypted and compressed script file.
 */
public class CreateScriptFile {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {

        String file = "test.txt";
        String cipher = "AES";
        String filePassword = "password";
        String compressionAlgorithm = "DEFLATE";

        String url = "jdbc:h2:mem:test";
        String user = "sa", dbPassword = "sa";

        PrintWriter w = openScriptWriter(file,
                compressionAlgorithm,
                cipher, filePassword, "UTF-8");
        w.println("create table test(id int primary key);");
        w.println("insert into test select x from system_range(1, 10);");
        w.close();

        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(url, user, dbPassword);
        RunScript.main(
                "-url", url,
                "-user", user, "-password", dbPassword,
                "-script", file,
                "-options",
                "compression", compressionAlgorithm,
                "cipher", cipher,
                "password", "'" + filePassword + "'"
        );
        Script.main(
                "-url", url,
                "-user", user, "-password", dbPassword,
                "-script", file,
                "-options",
                "compression", compressionAlgorithm,
                "cipher", cipher, "password", "'" + filePassword + "'"
        );
        conn.close();

        LineNumberReader r = openScriptReader(file,
                compressionAlgorithm,
                cipher, filePassword, "UTF-8");
        while (true) {
            String line = r.readLine();
            if (line == null) {
                break;
            }
            System.out.println(line);
        }
        r.close();

    }

    /**
     * Open a script writer.
     *
     * @param fileName the file name (the file will be overwritten)
     * @param compressionAlgorithm the compression algorithm (uppercase)
     * @param cipher the encryption algorithm or null
     * @param password the encryption password
     * @param charset the character set (for example UTF-8)
     * @return the print writer
     */
    public static PrintWriter openScriptWriter(String fileName,
            String compressionAlgorithm,
            String cipher, String password,
            String charset) throws IOException {
        try {
            OutputStream out;
            if (cipher != null) {
                byte[] key = SHA256.getKeyPasswordHash("script", password.toCharArray());
                FileUtils.delete(fileName);
                FileStore store = FileStore.open(null, fileName, "rw", cipher, key);
                store.init();
                out = new FileStoreOutputStream(store, null, compressionAlgorithm);
                out = new BufferedOutputStream(out, Constants.IO_BUFFER_SIZE_COMPRESS);
            } else {
                out = FileUtils.newOutputStream(fileName, false);
                out = new BufferedOutputStream(out, Constants.IO_BUFFER_SIZE);
                out = CompressTool.wrapOutputStream(out,
                        compressionAlgorithm, "script.sql");
            }
            return new PrintWriter(new OutputStreamWriter(out, charset));
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Open a script reader.
     *
     * @param fileName the file name (the file will be overwritten)
     * @param compressionAlgorithm the compression algorithm (uppercase)
     * @param cipher the encryption algorithm or null
     * @param password the encryption password
     * @param charset the character set (for example UTF-8)
     * @return the script reader
     */
    public static LineNumberReader openScriptReader(String fileName,
            String compressionAlgorithm,
            String cipher, String password,
            String charset) throws IOException {
        try {
            InputStream in;
            if (cipher != null) {
                byte[] key = SHA256.getKeyPasswordHash("script", password.toCharArray());
                FileStore store = FileStore.open(null, fileName, "rw", cipher, key);
                store.init();
                in = new FileStoreInputStream(store, null,
                        compressionAlgorithm != null, false);
                in = new BufferedInputStream(in, Constants.IO_BUFFER_SIZE_COMPRESS);
            } else {
                in = FileUtils.newInputStream(fileName);
                in = new BufferedInputStream(in, Constants.IO_BUFFER_SIZE);
                in = CompressTool.wrapInputStream(in, compressionAlgorithm, "script.sql");
                if (in == null) {
                    throw new IOException("Entry not found: script.sql in " + fileName);
                }
            }
            return new LineNumberReader(new InputStreamReader(in, charset));
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

}
