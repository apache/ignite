/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.h2.util.New;

/**
 * A page parser can parse an HTML page and replace the tags there.
 * This class is used by the H2 Console.
 */
public class PageParser {
    private static final int TAB_WIDTH = 4;

    private final String page;
    private int pos;
    private final Map<String, Object> settings;
    private final int len;
    private StringBuilder result;

    private PageParser(String page, Map<String, Object> settings, int pos) {
        this.page = page;
        this.pos = pos;
        this.len = page.length();
        this.settings = settings;
        result = new StringBuilder(len);
    }

    /**
     * Replace the tags in the HTML page with the given settings.
     *
     * @param page the HTML page
     * @param settings the settings
     * @return the converted page
     */
    public static String parse(String page, Map<String, Object> settings) {
        PageParser block = new PageParser(page, settings, 0);
        return block.replaceTags();
    }

    private void setError(int i) {
        String s = page.substring(0, i) + "####BUG####" + page.substring(i);
        s = PageParser.escapeHtml(s);
        result = new StringBuilder();
        result.append(s);
    }

    private String parseBlockUntil(String end) throws ParseException {
        PageParser block = new PageParser(page, settings, pos);
        block.parseAll();
        if (!block.readIf(end)) {
            throw new ParseException(page, block.pos);
        }
        pos = block.pos;
        return block.result.toString();
    }

    private String replaceTags() {
        try {
            parseAll();
            if (pos != len) {
                setError(pos);
            }
        } catch (ParseException e) {
            setError(pos);
        }
        return result.toString();
    }

    @SuppressWarnings("unchecked")
    private void parseAll() throws ParseException {
        StringBuilder buff = result;
        String p = page;
        int i = pos;
        for (; i < len; i++) {
            char c = p.charAt(i);
            switch (c) {
            case '<': {
                if (p.charAt(i + 3) == ':' && p.charAt(i + 1) == '/') {
                    // end tag
                    pos = i;
                    return;
                } else if (p.charAt(i + 2) == ':') {
                    pos = i;
                    if (readIf("<c:forEach")) {
                        String var = readParam("var");
                        String items = readParam("items");
                        read(">");
                        int start = pos;
                        List<Object> list = (List<Object>) get(items);
                        if (list == null) {
                            result.append("?items?");
                            list = New.arrayList();
                        }
                        if (list.isEmpty()) {
                            parseBlockUntil("</c:forEach>");
                        }
                        for (Object o : list) {
                            settings.put(var, o);
                            pos = start;
                            String block = parseBlockUntil("</c:forEach>");
                            result.append(block);
                        }
                    } else if (readIf("<c:if")) {
                        String test = readParam("test");
                        int eq = test.indexOf("=='");
                        if (eq < 0) {
                            setError(i);
                            return;
                        }
                        String val = test.substring(eq + 3, test.length() - 1);
                        test = test.substring(0, eq);
                        String value = (String) get(test);
                        read(">");
                        String block = parseBlockUntil("</c:if>");
                        pos--;
                        if (value.equals(val)) {
                            result.append(block);
                        }
                    } else {
                        setError(i);
                        return;
                    }
                    i = pos;
                } else {
                    buff.append(c);
                }
                break;
            }
            case '$':
                if (p.length() > i + 1 && p.charAt(i + 1) == '{') {
                    i += 2;
                    int j = p.indexOf('}', i);
                    if (j < 0) {
                        setError(i);
                        return;
                    }
                    String item = p.substring(i, j).trim();
                    i = j;
                    String s = (String) get(item);
                    replaceTags(s);
                } else {
                    buff.append(c);
                }
                break;
            default:
                buff.append(c);
                break;
            }
        }
        pos = i;
    }

    @SuppressWarnings("unchecked")
    private Object get(String item) {
        int dot = item.indexOf('.');
        if (dot >= 0) {
            String sub = item.substring(dot + 1);
            item = item.substring(0, dot);
            HashMap<String, Object> map = (HashMap<String, Object>) settings.get(item);
            if (map == null) {
                return "?" + item + "?";
            }
            return map.get(sub);
        }
        return settings.get(item);
    }

    private void replaceTags(String s) {
        if (s != null) {
            result.append(PageParser.parse(s, settings));
        }
    }

    private String readParam(String name) throws ParseException {
        read(name);
        read("=");
        read("\"");
        int start = pos;
        while (page.charAt(pos) != '"') {
            pos++;
        }
        int end = pos;
        read("\"");
        String s = page.substring(start, end);
        return PageParser.parse(s, settings);
    }

    private void skipSpaces() {
        while (page.charAt(pos) == ' ') {
            pos++;
        }
    }

    private void read(String s) throws ParseException {
        if (!readIf(s)) {
            throw new ParseException(s, pos);
        }
    }

    private boolean readIf(String s) {
        skipSpaces();
        if (page.regionMatches(pos, s, 0, s.length())) {
            pos += s.length();
            skipSpaces();
            return true;
        }
        return false;
    }

    /**
     * Convert data to HTML, but don't convert newlines and multiple spaces.
     *
     * @param s the data
     * @return the escaped html text
     */
    static String escapeHtmlData(String s) {
        return escapeHtml(s, false);
    }

    /**
     * Convert data to HTML, including newlines and multiple spaces.
     *
     * @param s the data
     * @return the escaped html text
     */
    public static String escapeHtml(String s) {
        return escapeHtml(s, true);
    }

    private static String escapeHtml(String s, boolean convertBreakAndSpace) {
        if (s == null) {
            return null;
        }
        if (convertBreakAndSpace) {
            if (s.length() == 0) {
                return "&nbsp;";
            }
        }
        StringBuilder buff = new StringBuilder(s.length());
        boolean convertSpace = true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ' ' || c == '\t') {
                // convert tabs into spaces
                for (int j = 0; j < (c == ' ' ? 1 : TAB_WIDTH); j++) {
                    if (convertSpace && convertBreakAndSpace) {
                        buff.append("&nbsp;");
                    } else {
                        buff.append(' ');
                        convertSpace = true;
                    }
                }
                continue;
            }
            convertSpace = false;
            switch (c) {
            case '$':
                // so that ${ } in the text is interpreted correctly
                buff.append("&#36;");
                break;
            case '<':
                buff.append("&lt;");
                break;
            case '>':
                buff.append("&gt;");
                break;
            case '&':
                buff.append("&amp;");
                break;
            case '"':
                buff.append("&quot;");
                break;
            case '\'':
                buff.append("&#39;");
                break;
            case '\n':
                if (convertBreakAndSpace) {
                    buff.append("<br />");
                    convertSpace = true;
                } else {
                    buff.append(c);
                }
                break;
            default:
                if (c >= 128) {
                    buff.append("&#").append((int) c).append(';');
                } else {
                    buff.append(c);
                }
                break;
            }
        }
        return buff.toString();
    }

    /**
     * Escape text as a the javascript string.
     *
     * @param s the text
     * @return the javascript string
     */
    static String escapeJavaScript(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() == 0) {
            return "";
        }
        StringBuilder buff = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
            case '"':
                buff.append("\\\"");
                break;
            case '\'':
                buff.append("\\'");
                break;
            case '\\':
                buff.append("\\\\");
                break;
            case '\n':
                buff.append("\\n");
                break;
            case '\r':
                buff.append("\\r");
                break;
            case '\t':
                buff.append("\\t");
                break;
            default:
                buff.append(c);
                break;
            }
        }
        return buff.toString();
    }
}
