/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.h2.engine.Constants;
import org.h2.util.StringUtils;

/**
 * This application merges the html documentation to one file
 * (onePage.html), so that the PDF document can be created.
 */
public class MergeDocs {

    private static final String BASE_DIR = "docs/html";

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        // the order of pages is important here
        String[] pages = { "quickstart.html", "installation.html",
                "tutorial.html", "features.html", "performance.html",
                "advanced.html", "grammar.html", "functions.html",
                "datatypes.html", "systemtables.html",
                "build.html", "history.html", "faq.html" };
        StringBuilder buff = new StringBuilder();
        for (String fileName : pages) {
            String text = getContent(fileName);
            for (String page : pages) {
                text = StringUtils.replaceAll(text, page + "#", "#");
            }
            text = disableRailroads(text);
            text = removeHeaderFooter(fileName, text);
            text = fixLinks(text);
            text = fixTableBorders(text);
            buff.append(text);
        }
        String finalText = buff.toString();
        File output = new File(BASE_DIR, "onePage.html");
        PrintWriter writer = new PrintWriter(new FileWriter(output));
        writer.println("<html><head><meta http-equiv=\"Content-Type\" " +
                "content=\"text/html;charset=utf-8\" /><title>");
        writer.println("H2 Documentation");
        writer.println("</title><link rel=\"stylesheet\" type=\"text/css\" " +
                "href=\"stylesheetPdf.css\" /></head><body>");
        writer.println("<p class=\"title\">H2 Database Engine</p>");
        writer.println("<p>Version " + Constants.getFullVersion() + "</p>");
        writer.println(finalText);
        writer.println("</body></html>");
        writer.close();
    }

    private static String disableRailroads(String text) {
        text = StringUtils.replaceAll(text,
                "<!-- railroad-start -->",
                "<!-- railroad-start ");
        text = StringUtils.replaceAll(text,
                "<!-- railroad-end -->",
                " railroad-end -->");
        text = StringUtils.replaceAll(text,
                "<!-- syntax-start",
                "<!-- syntax-start -->");
        text = StringUtils.replaceAll(text,
                "syntax-end -->",
                "<!-- syntax-end -->");
        return text;
    }

    private static String removeHeaderFooter(String fileName, String text) {
        // String start = "<body";
        // String end = "</body>";

        String start = "<!-- } -->";
        String end = "<!-- [close] { --></div></td></tr></table>" +
                "<!-- } --><!-- analytics --></body></html>";

        int idx = text.indexOf(end);
        if (idx < 0) {
            throw new RuntimeException("Footer not found in file " + fileName);
        }
        text = text.substring(0, idx);
        idx = text.indexOf(start) + start.length();
        text = text.substring(idx + 1);
        return text;
    }

    private static String fixLinks(String text) {
        return text
                .replaceAll("href=\"build.html\"", "href=\"#build_index\"")
                .replaceAll("href=\"datatypes.html\"", "href=\"#datatypes_index\"")
                .replaceAll("href=\"faq.html\"", "href=\"#faq_index\"")
                .replaceAll("href=\"grammar.html\"", "href=\"#grammar_index\"")
                .replaceAll("href=\"tutorial.html\"", "href=\"#tutorial_index\"");
    }

    private static String fixTableBorders(String text) {
        return text
                .replaceAll("<table class=\"main\">",
                        "<table class=\"main\" border=\"1\" cellpadding=\"5\" cellspacing=\"0\">");
    }

    private static String getContent(String fileName) throws Exception {
        File file = new File(BASE_DIR, fileName);
        int length = (int) file.length();
        char[] data = new char[length];
        FileReader reader = new FileReader(file);
        int off = 0;
        while (length > 0) {
            int len = reader.read(data, off, length);
            off += len;
            length -= len;
        }
        reader.close();
        String s = new String(data);
        return s;
    }
}
