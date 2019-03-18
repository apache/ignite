/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.i18n;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Properties;
import org.h2.build.code.CheckTextFiles;
import org.h2.build.indexer.HtmlConverter;
import org.h2.util.IOUtils;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;

/**
 * This class converts a file stored in the UTF-8 encoding format to
 * a properties file and vice versa.
 */
public class PropertiesToUTF8 {

    private PropertiesToUTF8() {
        // utility class
    }

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        convert("bin/org/h2/res");
        convert("bin/org/h2/server/web/res");
    }

    /**
     * Convert a properties file to a UTF-8 text file.
     *
     * @param source the name of the properties file
     * @param target the target file name
     */
    static void propertiesToTextUTF8(String source, String target)
            throws Exception {
        if (!new File(source).exists()) {
            return;
        }
        Properties prop = SortedProperties.loadProperties(source);
        FileOutputStream out = new FileOutputStream(target);
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        // keys is sorted
        for (Enumeration<Object> en = prop.keys(); en.hasMoreElements();) {
            String key = (String) en.nextElement();
            String value = prop.getProperty(key, null);
            writer.print("@" + key + "\n");
            writer.print(value + "\n\n");
        }
        writer.close();
    }

    /**
     * Convert a translation file (in UTF-8) to a properties file (without
     * special characters).
     *
     * @param source the source file name
     * @param target the target file name
     */
    static void textUTF8ToProperties(String source, String target)
            throws Exception {
        if (!new File(source).exists()) {
            return;
        }
        try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(
                new FileInputStream(source), StandardCharsets.UTF_8))) {
            SortedProperties prop = new SortedProperties();
            StringBuilder buff = new StringBuilder();
            String key = null;
            boolean found = false;
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                if (line.startsWith("@")) {
                    if (key != null) {
                        prop.setProperty(key, buff.toString());
                        buff.setLength(0);
                    }
                    found = true;
                    key = line.substring(1);
                } else {
                    if (buff.length() > 0) {
                        buff.append(System.getProperty("line.separator"));
                    }
                    buff.append(line);
                }
            }
            if (found) {
                prop.setProperty(key, buff.toString());
            }
            prop.store(target);
        }
    }

    private static void convert(String source) throws Exception {
        for (File f : new File(source).listFiles()) {
            if (!f.getName().endsWith(".properties")) {
                continue;
            }
            FileInputStream in = new FileInputStream(f);
            InputStreamReader r = new InputStreamReader(in, StandardCharsets.UTF_8);
            String s = IOUtils.readStringAndClose(r, -1);
            in.close();
            String name = f.getName();
            String utf8, html;
            if (name.startsWith("utf8")) {
                utf8 = HtmlConverter.convertHtmlToString(s);
                html = HtmlConverter.convertStringToHtml(utf8);
                RandomAccessFile out = new RandomAccessFile("_" + name.substring(4), "rw");
                out.write(html.getBytes());
                out.setLength(out.getFilePointer());
                out.close();
            } else {
                new CheckTextFiles().checkOrFixFile(f, false, false);
                html = s;
                utf8 = HtmlConverter.convertHtmlToString(html);
                // s = unescapeHtml(s);
                utf8 = StringUtils.javaDecode(utf8);
                FileOutputStream out = new FileOutputStream("_utf8" + f.getName());
                OutputStreamWriter w = new OutputStreamWriter(out, StandardCharsets.UTF_8);
                w.write(utf8);
                w.close();
                out.close();
            }
            String java = StringUtils.javaEncode(utf8);
            java = StringUtils.replaceAll(java, "\\r", "\r");
            java = StringUtils.replaceAll(java, "\\n", "\n");
            RandomAccessFile out = new RandomAccessFile("_java." + name, "rw");
            out.write(java.getBytes());
            out.setLength(out.getFilePointer());
            out.close();
        }
    }

}
