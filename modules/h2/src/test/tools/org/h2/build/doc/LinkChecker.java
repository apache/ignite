/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.h2.tools.Server;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * The link checker makes sure that each link in the documentation
 * points to an existing target.
 */
public class LinkChecker {

    private static final boolean TEST_EXTERNAL_LINKS = false;
    private static final boolean OPEN_EXTERNAL_LINKS = false;
    private static final String[] IGNORE_MISSING_LINKS_TO = {
        "SysProperties", "ErrorCode",
        // TODO check these replacement link too
        "#build_index",
        "#datatypes_index",
        "#faq_index",
        "#grammar_index",
        "#tutorial_index"
    };

    private final HashMap<String, String> targets = new HashMap<>();
    private final HashMap<String, String> links = new HashMap<>();

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new LinkChecker().run(args);
    }

    private void run(String... args) throws Exception {
        String dir = "docs";
        for (int i = 0; i < args.length; i++) {
            if ("-dir".equals(args[i])) {
                dir = args[++i];
            }
        }
        process(dir);
        listExternalLinks();
        listBadLinks();
    }

    private void listExternalLinks() {
        for (String link : links.keySet()) {
            if (link.startsWith("http")) {
                if (link.indexOf("//localhost") > 0) {
                    continue;
                }
                if (TEST_EXTERNAL_LINKS) {
                    try {
                        URL url = new URL(link);
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setConnectTimeout(2000);
                        conn.setRequestMethod("GET");
                        HttpURLConnection.setFollowRedirects(false);
                        conn.connect();
                        int code = conn.getResponseCode();
                        String msg;
                        switch (code) {
                        case 200:
                            msg = "OK";
                            break;
                        case 301:
                            msg = "Moved Permanently";
                            break;
                        case 302:
                            msg = "Found";
                            break;
                        case 403:
                            msg = "Forbidden";
                            break;
                        case 404:
                            msg = "Not Found";
                            break;
                        case 500:
                            msg = "Internal Server Error";
                            break;
                        default:
                            msg = "?";
                        }
                        System.out.println(code + " " + msg + " " + link);
                        conn.getInputStream().close();
                    } catch (IOException e) {
                        System.out.println("link checker error " + e.toString() + " " + link);
                        // ignore
                    }
                }
                if (OPEN_EXTERNAL_LINKS) {
                    System.out.println(link);
                    try {
                        Server.openBrowser(link);
                        Thread.sleep(100);
                    } catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    }

    private void listBadLinks() throws Exception {
        ArrayList<String> errors = new ArrayList<>();
        for (String link : links.keySet()) {
            if (!link.startsWith("http") && !link.endsWith("h2.pdf")
                    && link.indexOf("_ja.") < 0) {
                if (targets.get(link) == null) {
                    errors.add(links.get(link) + ": Link missing " + link);
                }
            }
        }
        for (String link : links.keySet()) {
            if (!link.startsWith("http")) {
                targets.remove(link);
            }
        }
        for (String name : targets.keySet()) {
            if (targets.get(name).equals("id")) {
                boolean ignore = false;
                for (String to : IGNORE_MISSING_LINKS_TO) {
                    if (name.contains(to)) {
                        ignore = true;
                        break;
                    }
                }
                if (!ignore) {
                    errors.add("No link to " + name);
                }
            }
        }
        Collections.sort(errors);
        for (String error : errors) {
            System.out.println(error);
        }
        if (errors.size() > 0) {
            throw new Exception("Problems where found by the Link Checker");
        }
    }

    private void process(String path) throws Exception {
        if (path.endsWith("/CVS") || path.endsWith("/.svn")) {
            return;
        }
        File file = new File(path);
        if (file.isDirectory()) {
            for (String n : file.list()) {
                process(path + "/" + n);
            }
        } else {
            processFile(path);
        }
    }

    private void processFile(String path) throws Exception {
        targets.put(path, "file");
        String lower = StringUtils.toLowerEnglish(path);
        if (!lower.endsWith(".html") && !lower.endsWith(".htm")) {
            return;
        }
        String fileName = new File(path).getName();
        String parent = path.substring(0, path.lastIndexOf('/'));
        String html = IOUtils.readStringAndClose(new FileReader(path), -1);
        int idx = -1;
        while (true) {
            idx = html.indexOf(" id=\"", idx + 1);
            if (idx < 0) {
                break;
            }
            int start = idx + " id=\"".length();
            int end = html.indexOf('"', start);
            if (end < 0) {
                error(fileName, "Expected \" after id= " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start, end);
            if (!ref.startsWith("_")) {
                targets.put(path + "#" + ref, "id");
            }
        }
        idx = -1;
        while (true) {
            idx = html.indexOf(" href=\"", idx + 1);
            if (idx < 0) {
                break;
            }
            int start = html.indexOf('"', idx);
            if (start < 0) {
                error(fileName, "Expected \" after href= at " + html.substring(idx, idx + 100));
            }
            int end = html.indexOf('"', start + 1);
            if (end < 0) {
                error(fileName, "Expected \" after href= at " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start + 1, end);
            if (ref.startsWith("http:") || ref.startsWith("https:")) {
                // ok
            } else if (ref.startsWith("javascript:")) {
                ref = null;
                // ok
            } else if (ref.length() == 0) {
                ref = null;
                // ok
            } else if (ref.startsWith("#")) {
                ref = path + ref;
            } else {
                String p = parent;
                while (ref.startsWith(".")) {
                    if (ref.startsWith("./")) {
                        ref = ref.substring(2);
                    } else if (ref.startsWith("../")) {
                        ref = ref.substring(3);
                        p = p.substring(0, p.lastIndexOf('/'));
                    }
                }
                ref = p + "/" + ref;
            }
            if (ref != null) {
                links.put(ref, path);
            }
        }
        idx = -1;
        while (true) {
            idx = html.indexOf("<a ", idx + 1);
            if (idx < 0) {
                break;
            }
            int equals = html.indexOf('=', idx);
            if (equals < 0) {
                error(fileName, "Expected = after <a at " + html.substring(idx, idx + 100));
            }
            String type = html.substring(idx + 2, equals).trim();
            int start = html.indexOf('"', idx);
            if (start < 0) {
                error(fileName, "Expected \" after <a at " + html.substring(idx, idx + 100));
            }
            int end = html.indexOf('"', start + 1);
            if (end < 0) {
                error(fileName, "Expected \" after <a at " + html.substring(idx, idx + 100));
            }
            String ref = html.substring(start + 1, end);
            if (type.equals("href")) {
                // already checked
            } else if (type.equals("id")) {
                targets.put(path + "#" + ref, "id");
            } else {
                error(fileName, "Unsupported <a ?: " + html.substring(idx, idx + 100));
            }
        }
    }

    private static void error(String fileName, String string) {
        System.out.println("ERROR with " + fileName + ": " + string);
    }

}
