/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.i18n;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Stack;
import org.h2.build.doc.XMLParser;
import org.h2.server.web.PageParser;
import org.h2.util.IOUtils;
import org.h2.util.SortedProperties;
import org.h2.util.StringUtils;

/**
 * This class updates the translation source code files by parsing
 * the HTML documentation. It also generates the translated HTML
 * documentation.
 */
public class PrepareTranslation {
    private static final String MAIN_LANGUAGE = "en";
    private static final String[] EXCLUDE = { "datatypes.html",
            "functions.html", "grammar.html" };

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        String baseDir = "src/docsrc/textbase";
        prepare(baseDir, "src/main/org/h2/res", true);
        prepare(baseDir, "src/main/org/h2/server/web/res", true);

        // convert the txt files to properties files
        PropertiesToUTF8.textUTF8ToProperties(
                "src/docsrc/text/_docs_de.utf8.txt",
                "src/docsrc/text/_docs_de.properties");
        PropertiesToUTF8.textUTF8ToProperties(
                "src/docsrc/text/_docs_ja.utf8.txt",
                "src/docsrc/text/_docs_ja.properties");

        // create the .jsp files and extract the text in the main language
        extractFromHtml("docs/html", "src/docsrc/text");

        // add missing translations and create a new baseline
        prepare(baseDir, "src/docsrc/text", false);

        // create the translated documentation
        buildHtml("src/docsrc/text", "docs/html", "en");
        // buildHtml("src/docsrc/text", "docs/html", "de");
        // buildHtml("src/docsrc/text", "docs/html", "ja");

        // convert the properties files back to utf8 text files, including the
        // main language (to be used as a template)
        PropertiesToUTF8.propertiesToTextUTF8(
                "src/docsrc/text/_docs_en.properties",
                "src/docsrc/text/_docs_en.utf8.txt");
        PropertiesToUTF8.propertiesToTextUTF8(
                "src/docsrc/text/_docs_de.properties",
                "src/docsrc/text/_docs_de.utf8.txt");
        PropertiesToUTF8.propertiesToTextUTF8(
                "src/docsrc/text/_docs_ja.properties",
                "src/docsrc/text/_docs_ja.utf8.txt");

        // delete temporary files
        for (File f : new File("src/docsrc/text").listFiles()) {
            if (!f.getName().endsWith(".utf8.txt")) {
                f.delete();
            }
        }
    }

    private static void buildHtml(String templateDir, String targetDir,
            String language) throws IOException {
        File[] list = new File(templateDir).listFiles();
        new File(targetDir).mkdirs();
        // load the main 'translation'
        String propName = templateDir + "/_docs_" + MAIN_LANGUAGE
                + ".properties";
        Properties prop = load(propName, false);
        propName = templateDir + "/_docs_" + language + ".properties";
        if (!(new File(propName)).exists()) {
            throw new IOException("Translation not found: " + propName);
        }
        Properties transProp = load(propName, false);
        for (Object k : transProp.keySet()) {
            String key = (String) k;
            String t = transProp.getProperty(key);
            // overload with translations, but not the ones starting with #
            if (t.startsWith("##")) {
                prop.put(key, t.substring(2));
            } else if (!t.startsWith("#")) {
                prop.put(key, t);
            }
        }
        ArrayList <String>fileNames = new ArrayList<>();
        for (File f : list) {
            String name = f.getName();
            if (!name.endsWith(".jsp")) {
                continue;
            }
            // remove '.jsp'
            name = name.substring(0, name.length() - 4);
            fileNames.add(name);
        }
        for (File f : list) {
            String name = f.getName();
            if (!name.endsWith(".jsp")) {
                continue;
            }
            // remove '.jsp'
            name = name.substring(0, name.length() - 4);
            String template = IOUtils.readStringAndClose(new FileReader(
                    templateDir + "/" + name + ".jsp"), -1);
            HashMap<String, Object> map = new HashMap<>();
            for (Object k : prop.keySet()) {
                map.put(k.toString(), prop.get(k));
            }
            String html = PageParser.parse(template, map);
            html = StringUtils.replaceAll(html, "lang=\"" + MAIN_LANGUAGE
                    + "\"", "lang=\"" + language + "\"");
            for (String n : fileNames) {
                if ("frame".equals(n)) {
                    // don't translate 'frame.html' to 'frame_ja.html',
                    // otherwise we can't switch back to English
                    continue;
                }
                html = StringUtils.replaceAll(html, n + ".html\"", n + "_"
                        + language + ".html\"");
            }
            html = StringUtils.replaceAll(html,
                    "_" + MAIN_LANGUAGE + ".html\"", ".html\"");
            String target;
            if (language.equals(MAIN_LANGUAGE)) {
                target = targetDir + "/" + name + ".html";
            } else {
                target = targetDir + "/" + name + "_" + language + ".html";
            }
            OutputStream out = new FileOutputStream(target);
            OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write(html);
            writer.close();
        }
    }

    private static boolean exclude(String fileName) {
        for (String e : EXCLUDE) {
            if (fileName.endsWith(e)) {
                return true;
            }
        }
        return false;
    }

    private static void extractFromHtml(String dir, String target)
            throws Exception {
        for (File f : new File(dir).listFiles()) {
            String name = f.getName();
            if (!name.endsWith(".html")) {
                continue;
            }
            if (exclude(name)) {
                continue;
            }
            // remove '.html'
            name = name.substring(0, name.length() - 5);
            if (name.indexOf('_') >= 0) {
                // ignore translated files
                continue;
            }
            String template = extract(name, f, target);
            FileWriter writer = new FileWriter(target + "/" + name + ".jsp");
            writer.write(template);
            writer.close();
        }
    }

    // private static boolean isText(String s) {
    // if (s.length() < 2) {
    // return false;
    // }
    // for (int i = 0; i < s.length(); i++) {
    // char c = s.charAt(i);
    // if (!Character.isDigit(c) && c != '.' && c != '-' && c != '+') {
    // return true;
    // }
    // }
    // return false;
    // }

    private static String getSpace(String s, boolean start) {
        if (start) {
            for (int i = 0; i < s.length(); i++) {
                if (!Character.isSpaceChar(s.charAt(i))) {
                    if (i == 0) {
                        return "";
                    }
                    return s.substring(0, i);
                }
            }
            return s;
        }
        for (int i = s.length() - 1; i >= 0; i--) {
            if (!Character.isSpaceChar(s.charAt(i))) {
                if (i == s.length() - 1) {
                    return "";
                }
                return s.substring(i + 1, s.length());
            }
        }
        // if all spaces, return an empty string to avoid duplicate spaces
        return "";
    }

    private static String extract(String documentName, File f, String target)
            throws Exception {
        String xml = IOUtils.readStringAndClose(new InputStreamReader(
                new FileInputStream(f), StandardCharsets.UTF_8), -1);
        // the template contains ${} instead of text
        StringBuilder template = new StringBuilder(xml.length());
        int id = 0;
        SortedProperties prop = new SortedProperties();
        XMLParser parser = new XMLParser(xml);
        StringBuilder buff = new StringBuilder();
        Stack<String> stack = new Stack<>();
        String tag = "";
        boolean ignoreEnd = false;
        String nextKey = "";
        // for debugging
        boolean templateIsCopy = false;
        while (true) {
            int event = parser.next();
            if (event == XMLParser.END_DOCUMENT) {
                break;
            } else if (event == XMLParser.CHARACTERS) {
                String s = parser.getText();
                if (s.trim().length() == 0) {
                    if (buff.length() > 0) {
                        buff.append(s);
                    } else {
                        template.append(s);
                    }
                } else if ("p".equals(tag) || "li".equals(tag)
                        || "a".equals(tag) || "td".equals(tag)
                        || "th".equals(tag) || "h1".equals(tag)
                        || "h2".equals(tag) || "h3".equals(tag)
                        || "h4".equals(tag) || "body".equals(tag)
                        || "b".equals(tag) || "code".equals(tag)
                        || "form".equals(tag) || "span".equals(tag)
                        || "em".equals(tag) || "div".equals(tag)
                        || "strong".equals(tag) || "label".equals(tag)) {
                    if (buff.length() == 0) {
                        nextKey = documentName + "_" + (1000 + id++) + "_"
                                + tag;
                        template.append(getSpace(s, true));
                    } else if (templateIsCopy) {
                        buff.append(getSpace(s, true));
                    }
                    buff.append(s);
                } else if ("pre".equals(tag) || "title".equals(tag)
                        || "script".equals(tag) || "style".equals(tag)) {
                    // ignore, don't translate
                    template.append(s);
                } else {
                    System.out.println(f.getName()
                            + " invalid wrapper tag for text: " + tag
                            + " text: " + s);
                    System.out.println(parser.getRemaining());
                    throw new Exception();
                }
            } else if (event == XMLParser.START_ELEMENT) {
                stack.add(tag);
                String name = parser.getName();
                if ("code".equals(name) || "a".equals(name) || "b".equals(name)
                        || "span".equals(name)) {
                    // keep tags if wrapped, but not if this is the wrapper
                    if (buff.length() > 0) {
                        buff.append(parser.getToken());
                        ignoreEnd = false;
                    } else {
                        ignoreEnd = true;
                        template.append(parser.getToken());
                    }
                } else if ("p".equals(tag) || "li".equals(tag)
                        || "td".equals(tag) || "th".equals(tag)
                        || "h1".equals(tag) || "h2".equals(tag)
                        || "h3".equals(tag) || "h4".equals(tag)
                        || "body".equals(tag) || "form".equals(tag)) {
                    if (buff.length() > 0) {
                        if (templateIsCopy) {
                            template.append(buff.toString());
                        } else {
                            template.append("${" + nextKey + "}");
                        }
                        add(prop, nextKey, buff);
                    }
                    template.append(parser.getToken());
                } else {
                    template.append(parser.getToken());
                }
                tag = name;
            } else if (event == XMLParser.END_ELEMENT) {
                String name = parser.getName();
                if ("code".equals(name) || "a".equals(name) || "b".equals(name)
                        || "span".equals(name) || "em".equals(name) || "strong".equals(name)) {
                    if (ignoreEnd) {
                        if (buff.length() > 0) {
                            if (templateIsCopy) {
                                template.append(buff.toString());
                            } else {
                                template.append("${" + nextKey + "}");
                            }
                            add(prop, nextKey, buff);
                        }
                        template.append(parser.getToken());
                    } else {
                        if (buff.length() > 0) {
                            buff.append(parser.getToken());
                        }
                    }
                } else {
                    if (buff.length() > 0) {
                        if (templateIsCopy) {
                            template.append(buff.toString());
                        } else {
                            template.append("${" + nextKey + "}");
                        }
                        add(prop, nextKey, buff);
                    }
                    template.append(parser.getToken());
                }
                tag = stack.pop();
            } else if (event == XMLParser.DTD) {
                template.append(parser.getToken());
            } else if (event == XMLParser.COMMENT) {
                template.append(parser.getToken());
            } else {
                int eventType = parser.getEventType();
                throw new Exception("Unexpected event " + eventType + " at "
                        + parser.getRemaining());
            }
            // if(!xml.startsWith(template.toString())) {
            // System.out.println(nextKey);
            // System.out.println(template.substring(template.length()-60)
            // +";");
            // System.out.println(xml.substring(template.length()-60,
            // template.length()));
            // System.out.println(template.substring(template.length()-55)
            // +";");
            // System.out.println(xml.substring(template.length()-55,
            // template.length()));
            // break;
            // }
        }
        new File(target).mkdirs();
        String propFileName = target + "/_docs_" + MAIN_LANGUAGE + ".properties";
        Properties old = load(propFileName, false);
        prop.putAll(old);
        store(prop, propFileName, false);
        String t = template.toString();
        if (templateIsCopy && !t.equals(xml)) {
            for (int i = 0; i < Math.min(t.length(), xml.length()); i++) {
                if (t.charAt(i) != xml.charAt(i)) {
                    int start = Math.max(0, i - 30), end = Math.min(i + 30, xml.length());
                    t = t.substring(start, end);
                    xml = xml.substring(start, end);
                }
            }
            System.out.println("xml--------------------------------------------------: ");
            System.out.println(xml);
            System.out.println("t---------------------------------------------------: ");
            System.out.println(t);
            System.exit(1);
        }
        return t;
    }

    private static String clean(String text) {
        if (text.indexOf('\r') < 0 && text.indexOf('\n') < 0) {
            return text;
        }
        text = text.replace('\r', ' ');
        text = text.replace('\n', ' ');
        while (true) {
            String s = StringUtils.replaceAll(text, "  ", " ");
            if (s.equals(text)) {
                break;
            }
            text = s;
        }
        return text;
    }

    private static void add(Properties prop, String document, StringBuilder text) {
        String s = clean(text.toString());
        text.setLength(0);
        prop.setProperty(document, s);
    }

    private static void prepare(String baseDir, String path, boolean utf8)
            throws IOException {
        String suffix = utf8 ? ".prop" : ".properties";
        File dir = new File(path);
        File main = null;
        ArrayList<File> translations = new ArrayList<>();
        for (File f : dir.listFiles()) {
            if (f.getName().endsWith(suffix) && f.getName().indexOf('_') >= 0) {
                if (f.getName().endsWith("_" + MAIN_LANGUAGE + suffix)) {
                    main = f;
                } else {
                    translations.add(f);
                }
            }
        }
        SortedProperties p = load(main.getAbsolutePath(), utf8);
        Properties base = load(baseDir + "/" + main.getName(), utf8);
        store(p, main.getAbsolutePath(), utf8);
        for (File trans : translations) {
            String language = trans.getName();
            language = language.substring(language.lastIndexOf('_') + 1,
                    language.lastIndexOf('.'));
            prepare(p, base, trans, utf8);
        }
        store(p, baseDir + "/" + main.getName(), utf8);
    }

    private static SortedProperties load(String fileName, boolean utf8)
            throws IOException {
        if (utf8) {
            String s = new String(IOUtils.readBytesAndClose(
                    new FileInputStream(fileName), -1), StandardCharsets.UTF_8);
            return SortedProperties.fromLines(s);
        }
        return SortedProperties.loadProperties(fileName);
    }

    private static void store(SortedProperties p, String fileName, boolean utf8)
            throws IOException {
        if (utf8) {
            String s = p.toLines();
            FileOutputStream f = new FileOutputStream(fileName);
            f.write(s.getBytes(StandardCharsets.UTF_8));
            f.close();
        } else {
            p.store(fileName);
        }
    }

    private static void prepare(Properties main, Properties base, File trans,
            boolean utf8) throws IOException {
        SortedProperties p = load(trans.getAbsolutePath(), utf8);
        Properties oldTranslations = new Properties();
        for (Object k : base.keySet()) {
            String key = (String) k;
            String m = base.getProperty(key);
            String t = p.getProperty(key);
            if (t != null && !t.startsWith("#")) {
                oldTranslations.setProperty(m, t);
            }
        }
        HashSet<String> toTranslate = new HashSet<>();
        // add missing keys, using # and the value from the main file
        for (Object k : main.keySet()) {
            String key = (String) k;
            String now = main.getProperty(key);
            if (!p.containsKey(key)) {
                String t = oldTranslations.getProperty(now);
                if (t == null) {
                    // System.out.println(trans.getName() +
                    // ": key " + key + " not found in " +
                    // "translation file; added # 'translation'");
                    t = "#" + now;
                    p.put(key, t);
                } else {
                    p.put(key, t);
                }
            } else {
                String t = p.getProperty(key);
                String last = base.getProperty(key);
                if (t.startsWith("#") && !t.startsWith("##")) {
                    // not translated before
                    t = oldTranslations.getProperty(now);
                    if (t == null) {
                        t = "#" + now;
                    }
                    p.put(key, t);
                } else if (last != null && !last.equals(now)) {
                    t = oldTranslations.getProperty(now);
                    if (t == null) {
                        // main data changed since the last run: review
                        // translation
                        System.out.println(trans.getName() + ": key " + key
                                + " changed, please review; last=" + last
                                + " now=" + now);
                        String old = p.getProperty(key);
                        t = "#" + now + " #" + old;
                        p.put(key, t);
                    } else {
                        p.put(key, t);
                    }
                }
            }
        }
        for (String key : toTranslate) {
            String now = main.getProperty(key);
            String t;
            System.out
                    .println(trans.getName()
                            + ": key "
                            + key
                            + " not found in translation file; added dummy # 'translation'");
            t = "#" + now;
            p.put(key, t);
        }
        // remove keys that don't exist in the main file
        // (deleted or typo in the key)
        for (Object k : new ArrayList<>(p.keySet())) {
            String key = (String) k;
            if (!main.containsKey(key)) {
                p.remove(key);
            }
        }
        store(p, trans.getAbsolutePath(), utf8);
    }

}
