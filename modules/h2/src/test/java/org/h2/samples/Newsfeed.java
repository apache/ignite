/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.h2.tools.RunScript;
import org.h2.util.StringUtils;

/**
 * The newsfeed application uses XML functions to create an RSS and Atom feed
 * from a simple SQL script. A textual representation of the data is created as
 * well.
 */
public class Newsfeed {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        String targetDir = args.length == 0 ? "." : args[0];
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
        InputStream in = Newsfeed.class.getResourceAsStream("newsfeed.sql");
        ResultSet rs = RunScript.execute(conn, new InputStreamReader(in, StandardCharsets.ISO_8859_1));
        in.close();
        while (rs.next()) {
            String file = rs.getString("FILE");
            String content = rs.getString("CONTENT");
            if (file.endsWith(".txt")) {
                content = convertHtml2Text(content);
            }
            new File(targetDir).mkdirs();
            FileOutputStream out = new FileOutputStream(targetDir + "/" + file);
            Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write(content);
            writer.close();
            out.close();
        }
        conn.close();
    }

    /**
     * Convert HTML text to plain text.
     *
     * @param html the html text
     * @return the plain text
     */
    private static String convertHtml2Text(String html) {
        String s = html;
        s = StringUtils.replaceAll(s, "<b>", "");
        s = StringUtils.replaceAll(s, "</b>", "");
        s = StringUtils.replaceAll(s, "<ul>", "");
        s = StringUtils.replaceAll(s, "</ul>", "");
        s = StringUtils.replaceAll(s, "<li>", "- ");
        s = StringUtils.replaceAll(s, "</li>", "");
        s = StringUtils.replaceAll(s, "<a href=\"", "( ");
        s = StringUtils.replaceAll(s, "\">", " ) ");
        s = StringUtils.replaceAll(s, "</a>", "");
        s = StringUtils.replaceAll(s, "<br />", "");
        s = StringUtils.replaceAll(s, "<br/>", "");
        s = StringUtils.replaceAll(s, "<br>", "");
        if (s.indexOf('<') >= 0 || s.indexOf('>') >= 0) {
            throw new RuntimeException("Unsupported HTML Tag: < or > in " + s);
        }
        return s;
    }
}
