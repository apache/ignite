/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.SoftReference;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;

/**
 * A few String utility functions.
 */
public class StringUtils {

    private static SoftReference<String[]> softCache =
            new SoftReference<>(null);
    private static long softCacheCreatedNs;

    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final int[] HEX_DECODE = new int['f' + 1];

    // memory used by this cache:
    // 4 * 1024 * 2 (strings per pair) * 64 * 2 (bytes per char) = 0.5 MB
    private static final int TO_UPPER_CACHE_LENGTH = 2 * 1024;
    private static final int TO_UPPER_CACHE_MAX_ENTRY_LENGTH = 64;
    private static final String[][] TO_UPPER_CACHE = new String[TO_UPPER_CACHE_LENGTH][];

    static {
        for (int i = 0; i < HEX_DECODE.length; i++) {
            HEX_DECODE[i] = -1;
        }
        for (int i = 0; i <= 9; i++) {
            HEX_DECODE[i + '0'] = i;
        }
        for (int i = 0; i <= 5; i++) {
            HEX_DECODE[i + 'a'] = HEX_DECODE[i + 'A'] = i + 10;
        }
    }

    private StringUtils() {
        // utility class
    }

    private static String[] getCache() {
        String[] cache;
        // softCache can be null due to a Tomcat problem
        // a workaround is disable the system property org.apache.
        // catalina.loader.WebappClassLoader.ENABLE_CLEAR_REFERENCES
        if (softCache != null) {
            cache = softCache.get();
            if (cache != null) {
                return cache;
            }
        }
        // create a new cache at most every 5 seconds
        // so that out of memory exceptions are not delayed
        long time = System.nanoTime();
        if (softCacheCreatedNs != 0 && time - softCacheCreatedNs < TimeUnit.SECONDS.toNanos(5)) {
            return null;
        }
        try {
            cache = new String[SysProperties.OBJECT_CACHE_SIZE];
            softCache = new SoftReference<>(cache);
            return cache;
        } finally {
            softCacheCreatedNs = System.nanoTime();
        }
    }

    /**
     * Convert a string to uppercase using the English locale.
     *
     * @param s the test to convert
     * @return the uppercase text
     */
    public static String toUpperEnglish(String s) {
        if (s.length() > TO_UPPER_CACHE_MAX_ENTRY_LENGTH) {
            return s.toUpperCase(Locale.ENGLISH);
        }
        int index = s.hashCode() & (TO_UPPER_CACHE_LENGTH - 1);
        String[] e = TO_UPPER_CACHE[index];
        if (e != null) {
            if (e[0].equals(s)) {
                return e[1];
            }
        }
        String s2 = s.toUpperCase(Locale.ENGLISH);
        e = new String[] { s, s2 };
        TO_UPPER_CACHE[index] = e;
        return s2;
    }

    /**
     * Convert a string to lowercase using the English locale.
     *
     * @param s the text to convert
     * @return the lowercase text
     */
    public static String toLowerEnglish(String s) {
        return s.toLowerCase(Locale.ENGLISH);
    }

    /**
     * Check is a string starts with another string, ignoring the case.
     *
     * @param s the string to check (must be longer than start)
     * @param start the prefix of s
     * @return true if start is a prefix of s
     */
    public static boolean startsWithIgnoreCase(String s, String start) {
        if (s.length() < start.length()) {
            return false;
        }
        return s.substring(0, start.length()).equalsIgnoreCase(start);
    }

    /**
     * Convert a string to a SQL literal. Null is converted to NULL. The text is
     * enclosed in single quotes. If there are any special characters, the
     * method STRINGDECODE is used.
     *
     * @param s the text to convert.
     * @return the SQL literal
     */
    public static String quoteStringSQL(String s) {
        if (s == null) {
            return "NULL";
        }
        int length = s.length();
        StringBuilder buff = new StringBuilder(length + 2);
        buff.append('\'');
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '\'') {
                buff.append(c);
            } else if (c < ' ' || c > 127) {
                // need to start from the beginning because maybe there was a \
                // that was not quoted
                return "STRINGDECODE(" + quoteStringSQL(javaEncode(s)) + ")";
            }
            buff.append(c);
        }
        buff.append('\'');
        return buff.toString();
    }

    /**
     * Convert a string to a Java literal using the correct escape sequences.
     * The literal is not enclosed in double quotes. The result can be used in
     * properties files or in Java source code.
     *
     * @param s the text to convert
     * @return the Java representation
     */
    public static String javaEncode(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            switch (c) {
//            case '\b':
//                // BS backspace
//                // not supported in properties files
//                buff.append("\\b");
//                break;
            case '\t':
                // HT horizontal tab
                buff.append("\\t");
                break;
            case '\n':
                // LF linefeed
                buff.append("\\n");
                break;
            case '\f':
                // FF form feed
                buff.append("\\f");
                break;
            case '\r':
                // CR carriage return
                buff.append("\\r");
                break;
            case '"':
                // double quote
                buff.append("\\\"");
                break;
            case '\\':
                // backslash
                buff.append("\\\\");
                break;
            default:
                int ch = c & 0xffff;
                if (ch >= ' ' && (ch < 0x80)) {
                    buff.append(c);
                // not supported in properties files
                // } else if (ch < 0xff) {
                // buff.append("\\");
                // // make sure it's three characters (0x200 is octal 1000)
                // buff.append(Integer.toOctalString(0x200 | ch).substring(1));
                } else {
                    buff.append("\\u");
                    String hex = Integer.toHexString(ch);
                    // make sure it's four characters
                    for (int len = hex.length(); len < 4; len++) {
                        buff.append('0');
                    }
                    buff.append(hex);
                }
            }
        }
        return buff.toString();
    }

    /**
     * Add an asterisk ('[*]') at the given position. This format is used to
     * show where parsing failed in a statement.
     *
     * @param s the text
     * @param index the position
     * @return the text with asterisk
     */
    public static String addAsterisk(String s, int index) {
        if (s != null) {
            index = Math.min(index, s.length());
            s = s.substring(0, index) + "[*]" + s.substring(index);
        }
        return s;
    }

    private static DbException getFormatException(String s, int i) {
        return DbException.get(ErrorCode.STRING_FORMAT_ERROR_1, addAsterisk(s, i));
    }

    /**
     * Decode a text that is encoded as a Java string literal. The Java
     * properties file format and Java source code format is supported.
     *
     * @param s the encoded string
     * @return the string
     */
    public static String javaDecode(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (i + 1 >= s.length()) {
                    throw getFormatException(s, i);
                }
                c = s.charAt(++i);
                switch (c) {
                case 't':
                    buff.append('\t');
                    break;
                case 'r':
                    buff.append('\r');
                    break;
                case 'n':
                    buff.append('\n');
                    break;
                case 'b':
                    buff.append('\b');
                    break;
                case 'f':
                    buff.append('\f');
                    break;
                case '#':
                    // for properties files
                    buff.append('#');
                    break;
                case '=':
                    // for properties files
                    buff.append('=');
                    break;
                case ':':
                    // for properties files
                    buff.append(':');
                    break;
                case '"':
                    buff.append('"');
                    break;
                case '\\':
                    buff.append('\\');
                    break;
                case 'u': {
                    try {
                        c = (char) (Integer.parseInt(s.substring(i + 1, i + 5), 16));
                    } catch (NumberFormatException e) {
                        throw getFormatException(s, i);
                    }
                    i += 4;
                    buff.append(c);
                    break;
                }
                default:
                    if (c >= '0' && c <= '9') {
                        try {
                            c = (char) (Integer.parseInt(s.substring(i, i + 3), 8));
                        } catch (NumberFormatException e) {
                            throw getFormatException(s, i);
                        }
                        i += 2;
                        buff.append(c);
                    } else {
                        throw getFormatException(s, i);
                    }
                }
            } else {
                buff.append(c);
            }
        }
        return buff.toString();
    }

    /**
     * Convert a string to the Java literal and enclose it with double quotes.
     * Null will result in "null" (without double quotes).
     *
     * @param s the text to convert
     * @return the Java representation
     */
    public static String quoteJavaString(String s) {
        if (s == null) {
            return "null";
        }
        return "\"" + javaEncode(s) + "\"";
    }

    /**
     * Convert a string array to the Java source code that represents this
     * array. Null will be converted to 'null'.
     *
     * @param array the string array
     * @return the Java source code (including new String[]{})
     */
    public static String quoteJavaStringArray(String[] array) {
        if (array == null) {
            return "null";
        }
        StatementBuilder buff = new StatementBuilder("new String[]{");
        for (String a : array) {
            buff.appendExceptFirst(", ");
            buff.append(quoteJavaString(a));
        }
        return buff.append('}').toString();
    }

    /**
     * Convert an int array to the Java source code that represents this array.
     * Null will be converted to 'null'.
     *
     * @param array the int array
     * @return the Java source code (including new int[]{})
     */
    public static String quoteJavaIntArray(int[] array) {
        if (array == null) {
            return "null";
        }
        StatementBuilder buff = new StatementBuilder("new int[]{");
        for (int a : array) {
            buff.appendExceptFirst(", ");
            buff.append(a);
        }
        return buff.append('}').toString();
    }

    /**
     * Enclose a string with '(' and ')' if this is not yet done.
     *
     * @param s the string
     * @return the enclosed string
     */
    public static String enclose(String s) {
        if (s.startsWith("(")) {
            return s;
        }
        return "(" + s + ")";
    }

    /**
     * Remove enclosing '(' and ')' if this text is enclosed.
     *
     * @param s the potentially enclosed string
     * @return the string
     */
    public static String unEnclose(String s) {
        if (s.startsWith("(") && s.endsWith(")")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * Encode the string as an URL.
     *
     * @param s the string to encode
     * @return the encoded string
     */
    public static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (Exception e) {
            // UnsupportedEncodingException
            throw DbException.convert(e);
        }
    }

    /**
     * Decode the URL to a string.
     *
     * @param encoded the encoded URL
     * @return the decoded string
     */
    public static String urlDecode(String encoded) {
        int length = encoded.length();
        byte[] buff = new byte[length];
        int j = 0;
        for (int i = 0; i < length; i++) {
            char ch = encoded.charAt(i);
            if (ch == '+') {
                buff[j++] = ' ';
            } else if (ch == '%') {
                buff[j++] = (byte) Integer.parseInt(encoded.substring(i + 1, i + 3), 16);
                i += 2;
            } else {
                if (SysProperties.CHECK) {
                    if (ch > 127 || ch < ' ') {
                        throw new IllegalArgumentException(
                                "Unexpected char " + (int) ch + " decoding " + encoded);
                    }
                }
                buff[j++] = (byte) ch;
            }
        }
        return new String(buff, 0, j, StandardCharsets.UTF_8);
    }

    /**
     * Split a string into an array of strings using the given separator. A null
     * string will result in a null array, and an empty string in a zero element
     * array.
     *
     * @param s the string to split
     * @param separatorChar the separator character
     * @param trim whether each element should be trimmed
     * @return the array list
     */
    public static String[] arraySplit(String s, char separatorChar, boolean trim) {
        if (s == null) {
            return null;
        }
        int length = s.length();
        if (length == 0) {
            return new String[0];
        }
        ArrayList<String> list = New.arrayList();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == separatorChar) {
                String e = buff.toString();
                list.add(trim ? e.trim() : e);
                buff.setLength(0);
            } else if (c == '\\' && i < length - 1) {
                buff.append(s.charAt(++i));
            } else {
                buff.append(c);
            }
        }
        String e = buff.toString();
        list.add(trim ? e.trim() : e);
        return list.toArray(new String[0]);
    }

    /**
     * Combine an array of strings to one array using the given separator
     * character. A backslash and the separator character and escaped using a
     * backslash.
     *
     * @param list the string array
     * @param separatorChar the separator character
     * @return the combined string
     */
    public static String arrayCombine(String[] list, char separatorChar) {
        StatementBuilder buff = new StatementBuilder();
        for (String s : list) {
            buff.appendExceptFirst(String.valueOf(separatorChar));
            if (s == null) {
                s = "";
            }
            for (int j = 0, length = s.length(); j < length; j++) {
                char c = s.charAt(j);
                if (c == '\\' || c == separatorChar) {
                    buff.append('\\');
                }
                buff.append(c);
            }
        }
        return buff.toString();
    }

    /**
     * Creates an XML attribute of the form name="value".
     * A single space is prepended to the name,
     * so that multiple attributes can be concatenated.
     * @param name the attribute name
     * @param value the attribute value
     * @return the attribute
     */
    public static String xmlAttr(String name, String value) {
        return " " + name + "=\"" + xmlText(value) + "\"";
    }

    /**
     * Create an XML node with optional attributes and content.
     * The data is indented with 4 spaces if it contains a newline character.
     *
     * @param name the element name
     * @param attributes the attributes (may be null)
     * @param content the content (may be null)
     * @return the node
     */
    public static String xmlNode(String name, String attributes, String content) {
        return xmlNode(name, attributes, content, true);
    }

    /**
     * Create an XML node with optional attributes and content. The data is
     * indented with 4 spaces if it contains a newline character and the indent
     * parameter is set to true.
     *
     * @param name the element name
     * @param attributes the attributes (may be null)
     * @param content the content (may be null)
     * @param indent whether to indent the content if it contains a newline
     * @return the node
     */
    public static String xmlNode(String name, String attributes,
            String content, boolean indent) {
        String start = attributes == null ? name : name + attributes;
        if (content == null) {
            return "<" + start + "/>\n";
        }
        if (indent && content.indexOf('\n') >= 0) {
            content = "\n" + indent(content);
        }
        return "<" + start + ">" + content + "</" + name + ">\n";
    }

    /**
     * Indents a string with 4 spaces.
     *
     * @param s the string
     * @return the indented string
     */
    public static String indent(String s) {
        return indent(s, 4, true);
    }

    /**
     * Indents a string with spaces.
     *
     * @param s the string
     * @param spaces the number of spaces
     * @param newline append a newline if there is none
     * @return the indented string
     */
    public static String indent(String s, int spaces, boolean newline) {
        StringBuilder buff = new StringBuilder(s.length() + spaces);
        for (int i = 0; i < s.length();) {
            for (int j = 0; j < spaces; j++) {
                buff.append(' ');
            }
            int n = s.indexOf('\n', i);
            n = n < 0 ? s.length() : n + 1;
            buff.append(s, i, n);
            i = n;
        }
        if (newline && !s.endsWith("\n")) {
            buff.append('\n');
        }
        return buff.toString();
    }

    /**
     * Escapes a comment.
     * If the data contains '--', it is converted to '- -'.
     * The data is indented with 4 spaces if it contains a newline character.
     *
     * @param data the comment text
     * @return &lt;!-- data --&gt;
     */
    public static String xmlComment(String data) {
        int idx = 0;
        while (true) {
            idx = data.indexOf("--", idx);
            if (idx < 0) {
                break;
            }
            data = data.substring(0, idx + 1) + " " + data.substring(idx + 1);
        }
        // must have a space at the beginning and at the end,
        // otherwise the data must not contain '-' as the first/last character
        if (data.indexOf('\n') >= 0) {
            return "<!--\n" + indent(data) + "-->\n";
        }
        return "<!-- " + data + " -->\n";
    }

    /**
     * Converts the data to a CDATA element.
     * If the data contains ']]&gt;', it is escaped as a text element.
     *
     * @param data the text data
     * @return &lt;![CDATA[data]]&gt;
     */
    public static String xmlCData(String data) {
        if (data.contains("]]>")) {
            return xmlText(data);
        }
        boolean newline = data.endsWith("\n");
        data = "<![CDATA[" + data + "]]>";
        return newline ? data + "\n" : data;
    }

    /**
     * Returns &lt;?xml version="1.0"?&gt;
     * @return &lt;?xml version="1.0"?&gt;
     */
    public static String xmlStartDoc() {
        return "<?xml version=\"1.0\"?>\n";
    }

    /**
     * Escapes an XML text element.
     *
     * @param text the text data
     * @return the escaped text
     */
    public static String xmlText(String text) {
        return xmlText(text, false);
    }

    /**
     * Escapes an XML text element.
     *
     * @param text the text data
     * @param escapeNewline whether to escape newlines
     * @return the escaped text
     */
    public static String xmlText(String text, boolean escapeNewline) {
        int length = text.length();
        StringBuilder buff = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char ch = text.charAt(i);
            switch (ch) {
            case '<':
                buff.append("&lt;");
                break;
            case '>':
                buff.append("&gt;");
                break;
            case '&':
                buff.append("&amp;");
                break;
            case '\'':
                // &apos; is not valid in HTML
                buff.append("&#39;");
                break;
            case '\"':
                buff.append("&quot;");
                break;
            case '\r':
            case '\n':
                if (escapeNewline) {
                    buff.append("&#x").
                        append(Integer.toHexString(ch)).
                        append(';');
                } else {
                    buff.append(ch);
                }
                break;
            case '\t':
                buff.append(ch);
                break;
            default:
                if (ch < ' ' || ch > 127) {
                    buff.append("&#x").
                        append(Integer.toHexString(ch)).
                        append(';');
                } else {
                    buff.append(ch);
                }
            }
        }
        return buff.toString();
    }

    /**
     * Replace all occurrences of the before string with the after string. Unlike
     * {@link String#replaceAll(String, String)} this method reads {@code before}
     * and {@code after} arguments as plain strings and if {@code before} argument
     * is an empty string this method returns original string {@code s}.
     *
     * @param s the string
     * @param before the old text
     * @param after the new text
     * @return the string with the before string replaced
     */
    public static String replaceAll(String s, String before, String after) {
        int next = s.indexOf(before);
        if (next < 0 || before.isEmpty()) {
            return s;
        }
        StringBuilder buff = new StringBuilder(
                s.length() - before.length() + after.length());
        int index = 0;
        while (true) {
            buff.append(s, index, next).append(after);
            index = next + before.length();
            next = s.indexOf(before, index);
            if (next < 0) {
                buff.append(s, index, s.length());
                break;
            }
        }
        return buff.toString();
    }

    /**
     * Enclose a string with double quotes. A double quote inside the string is
     * escaped using a double quote.
     *
     * @param s the text
     * @return the double quoted text
     */
    public static String quoteIdentifier(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(length + 2);
        buff.append('\"');
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '"') {
                buff.append(c);
            }
            buff.append(c);
        }
        return buff.append('\"').toString();
    }

    /**
     * Check if a String is null or empty (the length is null).
     *
     * @param s the string to check
     * @return true if it is null or empty
     */
    public static boolean isNullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }

    /**
     * In a string, replace block comment marks with /++ .. ++/.
     *
     * @param sql the string
     * @return the resulting string
     */
    public static String quoteRemarkSQL(String sql) {
        sql = replaceAll(sql, "*/", "++/");
        return replaceAll(sql, "/*", "/++");
    }

    /**
     * Pad a string. This method is used for the SQL function RPAD and LPAD.
     *
     * @param string the original string
     * @param n the target length
     * @param padding the padding string
     * @param right true if the padding should be appended at the end
     * @return the padded string
     */
    public static String pad(String string, int n, String padding, boolean right) {
        if (n < 0) {
            n = 0;
        }
        if (n < string.length()) {
            return string.substring(0, n);
        } else if (n == string.length()) {
            return string;
        }
        char paddingChar;
        if (padding == null || padding.length() == 0) {
            paddingChar = ' ';
        } else {
            paddingChar = padding.charAt(0);
        }
        StringBuilder buff = new StringBuilder(n);
        n -= string.length();
        if (right) {
            buff.append(string);
        }
        for (int i = 0; i < n; i++) {
            buff.append(paddingChar);
        }
        if (!right) {
            buff.append(string);
        }
        return buff.toString();
    }

    /**
     * Create a new char array and copy all the data. If the size of the byte
     * array is zero, the same array is returned.
     *
     * @param chars the char array (may be null)
     * @return a new char array
     */
    public static char[] cloneCharArray(char[] chars) {
        if (chars == null) {
            return null;
        }
        int len = chars.length;
        if (len == 0) {
            return chars;
        }
        return Arrays.copyOf(chars, len);
    }

    /**
     * Trim a character from a string.
     *
     * @param s the string
     * @param leading if leading characters should be removed
     * @param trailing if trailing characters should be removed
     * @param sp what to remove (only the first character is used)
     *      or null for a space
     * @return the trimmed string
     */
    public static String trim(String s, boolean leading, boolean trailing,
            String sp) {
        char space = sp == null || sp.isEmpty() ? ' ' : sp.charAt(0);
        int begin = 0, end = s.length();
        if (leading) {
            while (begin < end && s.charAt(begin) == space) {
                begin++;
            }
        }
        if (trailing) {
            while (end > begin && s.charAt(end - 1) == space) {
                end--;
            }
        }
        // substring() returns self if start == 0 && end == length()
        return s.substring(begin, end);
    }

    /**
     * Get the string from the cache if possible. If the string has not been
     * found, it is added to the cache. If there is such a string in the cache,
     * that one is returned.
     *
     * @param s the original string
     * @return a string with the same content, if possible from the cache
     */
    public static String cache(String s) {
        if (!SysProperties.OBJECT_CACHE) {
            return s;
        }
        if (s == null) {
            return s;
        } else if (s.length() == 0) {
            return "";
        }
        int hash = s.hashCode();
        String[] cache = getCache();
        if (cache != null) {
            int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
            String cached = cache[index];
            if (cached != null) {
                if (s.equals(cached)) {
                    return cached;
                }
            }
            cache[index] = s;
        }
        return s;
    }

    /**
     * Clear the cache. This method is used for testing.
     */
    public static void clearCache() {
        softCache = new SoftReference<>(null);
    }

    /**
     * Convert a hex encoded string to a byte array.
     *
     * @param s the hex encoded string
     * @return the byte array
     */
    public static byte[] convertHexToBytes(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            throw DbException.get(ErrorCode.HEX_STRING_ODD_1, s);
        }
        len /= 2;
        byte[] buff = new byte[len];
        int mask = 0;
        int[] hex = HEX_DECODE;
        try {
            for (int i = 0; i < len; i++) {
                int d = hex[s.charAt(i + i)] << 4 | hex[s.charAt(i + i + 1)];
                mask |= d;
                buff[i] = (byte) d;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw DbException.get(ErrorCode.HEX_STRING_WRONG_1, s);
        }
        if ((mask & ~255) != 0) {
            throw DbException.get(ErrorCode.HEX_STRING_WRONG_1, s);
        }
        return buff;
    }

    /**
     * Convert a byte array to a hex encoded string.
     *
     * @param value the byte array
     * @return the hex encoded string
     */
    public static String convertBytesToHex(byte[] value) {
        return convertBytesToHex(value, value.length);
    }

    /**
     * Convert a byte array to a hex encoded string.
     *
     * @param value the byte array
     * @param len the number of bytes to encode
     * @return the hex encoded string
     */
    public static String convertBytesToHex(byte[] value, int len) {
        char[] buff = new char[len + len];
        char[] hex = HEX;
        for (int i = 0; i < len; i++) {
            int c = value[i] & 0xff;
            buff[i + i] = hex[c >> 4];
            buff[i + i + 1] = hex[c & 0xf];
        }
        return new String(buff);
    }

    /**
     * Check if this string is a decimal number.
     *
     * @param s the string
     * @return true if it is
     */
    public static boolean isNumber(String s) {
        if (s.length() == 0) {
            return false;
        }
        for (char c : s.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Append a zero-padded number to a string builder.
     *
     * @param buff the string builder
     * @param length the number of characters to append
     * @param positiveValue the number to append
     */
    public static void appendZeroPadded(StringBuilder buff, int length,
            long positiveValue) {
        if (length == 2) {
            if (positiveValue < 10) {
                buff.append('0');
            }
            buff.append(positiveValue);
        } else {
            String s = Long.toString(positiveValue);
            length -= s.length();
            while (length > 0) {
                buff.append('0');
                length--;
            }
            buff.append(s);
        }
    }

    /**
     * Escape table or schema patterns used for DatabaseMetaData functions.
     *
     * @param pattern the pattern
     * @return the escaped pattern
     */
    public static String escapeMetaDataPattern(String pattern) {
        if (pattern == null || pattern.length() == 0) {
            return pattern;
        }
        return replaceAll(pattern, "\\", "\\\\");
    }

}
