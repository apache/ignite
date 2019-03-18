/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.h2.samples.Newsfeed;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * Create the web site, mainly by copying the regular docs. A few items are
 * different in the web site, for example it calls web site analytics.
 * Also, the main entry point page is different.
 * The newsfeeds are generated here as well.
 */
public class WebSite {

    private static final String ANALYTICS_TAG = "<!-- analytics -->";
    private static final String ANALYTICS_SCRIPT =
        "<script src=\"http://www.google-analytics.com/ga.js\" " +
        "type=\"text/javascript\"></script>\n" +
        "<script type=\"text/javascript\">" +
        "var pageTracker=_gat._getTracker(\"UA-2351060-1\");" +
        "pageTracker._initData();pageTracker._trackPageview();" +
        "</script>";
    private static final String TRANSLATE_START = "<!-- translate";
    private static final String TRANSLATE_END = "translate -->";

    private static final String SOURCE_DIR = "docs";
    private static final String WEB_DIR = "../h2web";
    private final HashMap<String, String> fragments = new HashMap<>();

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new WebSite().run();
    }

    private void run() throws Exception {
        // create the web site
        deleteRecursive(new File(WEB_DIR));
        loadFragments();
        copy(new File(SOURCE_DIR), new File(WEB_DIR), true, true);
        Newsfeed.main(WEB_DIR + "/html");

        // create the internal documentation
        copy(new File(SOURCE_DIR), new File(SOURCE_DIR), true, false);
    }

    private void loadFragments() throws IOException {
        File dir = new File(SOURCE_DIR, "html");
        for (File f : dir.listFiles()) {
            if (f.getName().startsWith("fragments")) {
                FileInputStream in = new FileInputStream(f);
                byte[] bytes = IOUtils.readBytesAndClose(in, 0);
                String page = new String(bytes, StandardCharsets.UTF_8);
                fragments.put(f.getName(), page);
            }
        }
    }

    private String replaceFragments(String fileName, String page) {
        if (fragments.size() == 0) {
            return page;
        }
        String language = "";
        int index = fileName.indexOf('_');
        if (index >= 0) {
            int end = fileName.indexOf('.');
            language = fileName.substring(index, end);
        }
        String fragment = fragments.get("fragments" + language + ".html");
        int start = 0;
        while (true) {
            start = fragment.indexOf("<!-- [", start);
            if (start < 0) {
                break;
            }
            int endTag = fragment.indexOf("] { -->", start);
            int endBlock = fragment.indexOf("<!-- } -->", start);
            String tag = fragment.substring(start, endTag);
            String replacement = fragment.substring(start, endBlock);
            int pageStart = 0;
            while (true) {
                pageStart = page.indexOf(tag, pageStart);
                if (pageStart < 0) {
                    break;
                }
                int pageEnd = page.indexOf("<!-- } -->", pageStart);
                page = page.substring(0, pageStart) + replacement + page.substring(pageEnd);
                pageStart += replacement.length();
            }
            start = endBlock;
        }
        return page;
    }

    private void deleteRecursive(File dir) {
        if (dir.isDirectory()) {
            for (File f : dir.listFiles()) {
                deleteRecursive(f);
            }
        }
        dir.delete();
    }

    private void copy(File source, File target, boolean replaceFragments,
            boolean web) throws IOException {
        if (source.isDirectory()) {
            target.mkdirs();
            for (File f : source.listFiles()) {
                copy(f, new File(target, f.getName()), replaceFragments, web);
            }
        } else {
            String name = source.getName();
            if (name.endsWith("onePage.html") || name.startsWith("fragments")) {
                return;
            }
            if (web) {
                if (name.endsWith("main.html") || name.endsWith("main_ja.html")) {
                    return;
                }
            } else {
                if (name.endsWith("mainWeb.html") || name.endsWith("mainWeb_ja.html")) {
                    return;
                }
            }
            FileInputStream in = new FileInputStream(source);
            byte[] bytes = IOUtils.readBytesAndClose(in, 0);
            if (name.endsWith(".html")) {
                String page = new String(bytes, StandardCharsets.UTF_8);
                if (web) {
                    page = StringUtils.replaceAll(page, ANALYTICS_TAG, ANALYTICS_SCRIPT);
                }
                if (replaceFragments) {
                    page = replaceFragments(name, page);
                    page = StringUtils.replaceAll(page, "<a href=\"frame", "<a href=\"main");
                    page = StringUtils.replaceAll(page, "html/frame.html", "html/main.html");
                }
                if (web) {
                    page = StringUtils.replaceAll(page, TRANSLATE_START, "");
                    page = StringUtils.replaceAll(page, TRANSLATE_END, "");
                    page = StringUtils.replaceAll(page, "<pre>", "<pre class=\"notranslate\">");
                    page = StringUtils.replaceAll(page, "<code>", "<code class=\"notranslate\">");
                }
                bytes = page.getBytes(StandardCharsets.UTF_8);
            }
            FileOutputStream out = new FileOutputStream(target);
            out.write(bytes);
            out.close();
            if (web) {
                if (name.endsWith("mainWeb.html")) {
                    target.renameTo(new File(target.getParentFile(), "main.html"));
                } else if (name.endsWith("mainWeb_ja.html")) {
                    target.renameTo(new File(target.getParentFile(), "main_ja.html"));
                }
            }
        }
    }

}
