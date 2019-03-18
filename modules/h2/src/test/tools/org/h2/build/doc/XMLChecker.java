/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileReader;
import java.util.Stack;

import org.h2.util.IOUtils;

/**
 * This class checks that the HTML and XML part of the source code
 * is well-formed XML.
 */
public class XMLChecker {

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new XMLChecker().run(args);
    }

    private void run(String... args) throws Exception {
        String dir = ".";
        for (int i = 0; i < args.length; i++) {
            if ("-dir".equals(args[i])) {
                dir = args[++i];
            }
        }
        process(dir + "/src");
        process(dir + "/docs");
    }

    private void process(String path) throws Exception {
        if (path.endsWith("/CVS") || path.endsWith("/.svn")) {
            return;
        }
        File file = new File(path);
        if (file.isDirectory()) {
            for (String name : file.list()) {
                process(path + "/" + name);
            }
        } else {
            processFile(path);
        }
    }

    private static void processFile(String fileName) throws Exception {
        int idx = fileName.lastIndexOf('.');
        if (idx < 0) {
            return;
        }
        String suffix = fileName.substring(idx + 1);
        if (!suffix.equals("html") && !suffix.equals("xml") && !suffix.equals("jsp")) {
            return;
        }
        // System.out.println("Checking file:" + fileName);
        FileReader reader = new FileReader(fileName);
        String s = IOUtils.readStringAndClose(reader, -1);
        Exception last = null;
        try {
            checkXML(s, !suffix.equals("xml"));
        } catch (Exception e) {
            last = e;
            System.out.println("ERROR in file " + fileName + " " + e.toString());
        }
        if (last != null) {
            last.printStackTrace();
        }
    }

    private static void checkXML(String xml, boolean html) throws Exception {
        // String lastElement = null;
        // <li>: replace <li>([^\r]*[^<]*) with <li>$1</li>
        // use this for html file, for example if <li> is not closed
        String[] noClose = {};
        XMLParser parser = new XMLParser(xml);
        Stack<Object[]> stack = new Stack<>();
        boolean rootElement = false;
        while (true) {
            int event = parser.next();
            if (event == XMLParser.END_DOCUMENT) {
                break;
            } else if (event == XMLParser.START_ELEMENT) {
                if (stack.size() == 0) {
                    if (rootElement) {
                        throw new Exception("Second root element at " + parser.getRemaining());
                    }
                    rootElement = true;
                }
                String name = parser.getName();
                if (html) {
                    for (String n : noClose) {
                        if (name.equals(n)) {
                            name = null;
                            break;
                        }
                    }
                }
                if (name != null) {
                    stack.add(new Object[] { name, parser.getPos() });
                }
            } else if (event == XMLParser.END_ELEMENT) {
                String name = parser.getName();
                if (html) {
                    for (String n : noClose) {
                        if (name.equals(n)) {
                            throw new Exception("Unnecessary closing element "
                                    + name + " at " + parser.getRemaining());
                        }
                    }
                }
                while (true) {
                    Object[] pop = stack.pop();
                    String p = (String) pop[0];
                    if (p.equals(name)) {
                        break;
                    }
                    String remaining = xml.substring((Integer) pop[1]);
                    if (remaining.length() > 100) {
                        remaining = remaining.substring(0, 100);
                    }
                    throw new Exception("Unclosed element " + p + " at " + remaining);
                }
            } else if (event == XMLParser.CHARACTERS) {
                // lastElement = parser.getText();
            } else if (event == XMLParser.DTD) {
                // ignore
            } else if (event == XMLParser.COMMENT) {
                // ignore
            } else {
                int eventType = parser.getEventType();
                throw new Exception("Unexpected event " + eventType + " at "
                        + parser.getRemaining());
            }
        }
        if (stack.size() != 0) {
            throw new Exception("Unclosed root element");
        }
    }

}
