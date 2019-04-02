/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Random;
import org.h2.expression.function.DateTimeFunctions;
import org.h2.message.DbException;
import org.h2.test.TestBase;
import org.h2.test.utils.AssertThrows;
import org.h2.util.StringUtils;

/**
 * Tests string utility methods.
 */
public class TestStringUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testParseUInt31();
        testHex();
        testXML();
        testSplit();
        testJavaString();
        testURL();
        testPad();
        testReplaceAll();
        testTrim();
        testTrimSubstring();
    }

    private void testParseUInt31() {
        assertEquals(0, StringUtils.parseUInt31("101", 1, 2));
        assertEquals(11, StringUtils.parseUInt31("11", 0, 2));
        assertEquals(0, StringUtils.parseUInt31("000", 0, 3));
        assertEquals(1, StringUtils.parseUInt31("01", 0, 2));
        assertEquals(999999999, StringUtils.parseUInt31("X999999999", 1, 10));
        assertEquals(2147483647, StringUtils.parseUInt31("2147483647", 0, 10));
        testParseUInt31Bad(null, 0, 1);
        testParseUInt31Bad("1", -1, 1);
        testParseUInt31Bad("1", 0, 0);
        testParseUInt31Bad("12", 1, 0);
        testParseUInt31Bad("-0", 0, 2);
        testParseUInt31Bad("+0", 0, 2);
        testParseUInt31Bad("2147483648", 0, 10);
        testParseUInt31Bad("21474836470", 0, 11);
    }

    private void testParseUInt31Bad(String s, int start, int end) {
        try {
            StringUtils.parseUInt31(s, start, end);
        } catch (NullPointerException | IndexOutOfBoundsException | NumberFormatException e) {
            return;
        }
        fail();
    }

    private void testHex() {
        assertEquals("face",
                StringUtils.convertBytesToHex(new byte[]
                        { (byte) 0xfa, (byte) 0xce }));
        assertEquals(new byte[] { (byte) 0xfa, (byte) 0xce },
                StringUtils.convertHexToBytes("face"));
        assertEquals(new byte[] { (byte) 0xfa, (byte) 0xce },
                StringUtils.convertHexToBytes("fAcE"));
        assertEquals(new byte[] { (byte) 0xfa, (byte) 0xce },
                StringUtils.convertHexToBytes("FaCe"));
        new AssertThrows(DbException.class) { @Override
        public void test() {
            StringUtils.convertHexToBytes("120");
        }};
        new AssertThrows(DbException.class) { @Override
        public void test() {
            StringUtils.convertHexToBytes("fast");
        }};
        new AssertThrows(DbException.class) { @Override
        public void test() {
            StringUtils.convertHexToBytes("012=abcf");
        }};
    }

    private void testPad() {
        assertEquals("large", StringUtils.pad("larger text", 5, null, true));
        assertEquals("large", StringUtils.pad("larger text", 5, null, false));
        assertEquals("short+++++", StringUtils.pad("short", 10, "+", true));
        assertEquals("+++++short", StringUtils.pad("short", 10, "+", false));
    }

    private void testXML() {
        assertEquals("<!-- - - - - - -abc- - - - - - -->\n",
                StringUtils.xmlComment("------abc------"));
        assertEquals("<test/>\n",
                StringUtils.xmlNode("test", null, null));
        assertEquals("<test>Gr&#xfc;bel</test>\n",
                StringUtils.xmlNode("test", null,
                        StringUtils.xmlText("Gr\u00fcbel")));
        assertEquals("Rand&amp;Blue",
                StringUtils.xmlText("Rand&Blue"));
        assertEquals("&lt;&lt;[[[]]]&gt;&gt;",
                StringUtils.xmlCData("<<[[[]]]>>"));
        Date dt = DateTimeFunctions.parseDateTime(
                "2001-02-03 04:05:06 GMT",
                "yyyy-MM-dd HH:mm:ss z", "en", "GMT");
        String s = StringUtils.xmlStartDoc()
                + StringUtils.xmlComment("Test Comment")
                + StringUtils.xmlNode("rss",
                    StringUtils.xmlAttr("version", "2.0"),
                    StringUtils.xmlComment("Test Comment\nZeile2")
                    + StringUtils.xmlNode("channel", null,
                        StringUtils.xmlNode("title", null, "H2 Database Engine")
                        + StringUtils.xmlNode("link", null, "http://www.h2database.com")
                        + StringUtils.xmlNode("description", null, "H2 Database Engine")
                        + StringUtils.xmlNode("language", null, "en-us")
                        + StringUtils.xmlNode("pubDate", null,
                                DateTimeFunctions.formatDateTime(dt,
                                "EEE, d MMM yyyy HH:mm:ss z", "en", "GMT"))
                        + StringUtils.xmlNode("lastBuildDate", null,
                                DateTimeFunctions.formatDateTime(dt,
                                "EEE, d MMM yyyy HH:mm:ss z", "en", "GMT"))
                        + StringUtils.xmlNode("item", null,
                                StringUtils.xmlNode("title", null,
                                "New Version 0.9.9.9.9")
                                + StringUtils.xmlNode("link", null, "http://www.h2database.com")
                                + StringUtils.xmlNode("description", null,
                                        StringUtils.xmlCData("\nNew Features\nTest\n")))));
        assertEquals(
                s,
                "<?xml version=\"1.0\"?>\n"
                        + "<!-- Test Comment -->\n"
                        + "<rss version=\"2.0\">\n"
                        + "    <!--\n"
                        + "        Test Comment\n"
                        + "        Zeile2\n"
                        + "    -->\n"
                        + "    <channel>\n"
                        + "        <title>H2 Database Engine</title>\n"
                        + "        <link>http://www.h2database.com</link>\n"
                        + "        <description>H2 Database Engine</description>\n"
                        + "        <language>en-us</language>\n"
                        + "        <pubDate>Sat, 3 Feb 2001 04:05:06 GMT</pubDate>\n"
                        + "        <lastBuildDate>Sat, 3 Feb 2001 04:05:06 GMT</lastBuildDate>\n"
                        + "        <item>\n"
                        + "            <title>New Version 0.9.9.9.9</title>\n"
                        + "            <link>http://www.h2database.com</link>\n"
                        + "            <description>\n"
                        + "                <![CDATA[\n"
                        + "                New Features\n"
                        + "                Test\n" + "                ]]>\n"
                        + "            </description>\n" + "        </item>\n"
                        + "    </channel>\n" + "</rss>\n");
    }

    private void testURL() throws UnsupportedEncodingException {
        Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            int len = random.nextInt(10);
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < len; j++) {
                if (random.nextBoolean()) {
                    buff.append((char) random.nextInt(0x3000));
                } else {
                    buff.append((char) random.nextInt(255));
                }
            }
            String a = buff.toString();
            String b = URLEncoder.encode(a, "UTF-8");
            String c = URLDecoder.decode(b, "UTF-8");
            assertEquals(a, c);
            String d = StringUtils.urlDecode(b);
            assertEquals(d, c);
        }
    }

    private void testJavaString() {
        assertEquals("a\"b", StringUtils.javaDecode("a\"b"));
        Random random = new Random(1);
        for (int i = 0; i < 1000; i++) {
            int len = random.nextInt(10);
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < len; j++) {
                if (random.nextBoolean()) {
                    buff.append((char) random.nextInt(0x3000));
                } else {
                    buff.append((char) random.nextInt(255));
                }
            }
            String a = buff.toString();
            String b = StringUtils.javaEncode(a);
            String c = StringUtils.javaDecode(b);
            assertEquals(a, c);
        }
    }

    private void testSplit() {
        assertEquals(3,
                StringUtils.arraySplit("ABC,DEF,G\\,HI", ',', false).length);
        assertEquals(
                StringUtils.arrayCombine(new String[] { "", " ", "," }, ','),
                ", ,\\,");
        Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            int len = random.nextInt(10);
            StringBuilder buff = new StringBuilder();
            String select = "abcd,";
            for (int j = 0; j < len; j++) {
                char c = select.charAt(random.nextInt(select.length()));
                if (c == 'a') {
                    buff.append("\\\\");
                } else if (c == 'b') {
                    buff.append("\\,");
                } else {
                    buff.append(c);
                }
            }
            String a = buff.toString();
            String[] b = StringUtils.arraySplit(a, ',', false);
            String c = StringUtils.arrayCombine(b, ',');
            assertEquals(a, c);
        }
    }

    private void testReplaceAll() {
        assertEquals("def",
                StringUtils.replaceAll("abc def", "abc ", ""));
        assertEquals("af",
                StringUtils.replaceAll("abc def", "bc de", ""));
        assertEquals("abc def",
                StringUtils.replaceAll("abc def", "bc ", "bc "));
        assertEquals("abc ",
                StringUtils.replaceAll("abc def", "def", ""));
        assertEquals(" ",
                StringUtils.replaceAll("abc abc", "abc", ""));
        assertEquals("xyz xyz",
                StringUtils.replaceAll("abc abc", "abc", "xyz"));
        assertEquals("abc def",
                StringUtils.replaceAll("abc def", "xyz", "abc"));
        assertEquals("",
                StringUtils.replaceAll("abcabcabc", "abc", ""));
        assertEquals("abcabcabc",
                StringUtils.replaceAll("abcabcabc", "aBc", ""));
        assertEquals("abcabcabc",
                StringUtils.replaceAll("abcabcabc", "", "abc"));
    }

    private void testTrim() {
        assertEquals("a a",
                StringUtils.trim("a a", true, true, null));
        assertEquals("  a a  ",
                StringUtils.trim("  a a  ", false, false, null));
        assertEquals("  a a",
                StringUtils.trim("  a a  ", false, true, null));
        assertEquals("a a  ",
                StringUtils.trim("  a a  ", true, false, null));
        assertEquals("a a",
                StringUtils.trim("  a a  ", true, true, null));
        assertEquals("a a",
                StringUtils.trim("  a a  ", true, true, ""));
        assertEquals("zzbbzz",
                StringUtils.trim("zzbbzz", false, false, "z"));
        assertEquals("zzbb",
                StringUtils.trim("zzbbzz", false, true, "z"));
        assertEquals("bbzz",
                StringUtils.trim("zzbbzz", true, false, "z"));
        assertEquals("bb",
                StringUtils.trim("zzbbzz", true, true, "z"));
    }

    private void testTrimSubstring() {
        testTrimSubstringImpl("", "", 0, 0);
        testTrimSubstringImpl("", "    ", 0, 0);
        testTrimSubstringImpl("", "    ", 4, 4);
        testTrimSubstringImpl("select", " select  from", 1, 7);
        testTrimSubstringImpl("a b", " a b ", 1, 4);
        testTrimSubstringImpl("a b", " a b ", 1, 5);
        testTrimSubstringImpl("b", " a b ", 2, 5);
        new AssertThrows(StringIndexOutOfBoundsException.class) { @Override
            public void test() { StringUtils.trimSubstring(" with (", 1, 8); }
        };
    }

    private void testTrimSubstringImpl(String expected, String string, int startIndex, int endIndex) {
        assertEquals(expected, StringUtils.trimSubstring(string, startIndex, endIndex));
        assertEquals(expected, StringUtils
                .trimSubstring(new StringBuilder(endIndex - startIndex), string, startIndex, endIndex).toString());
    }

}
