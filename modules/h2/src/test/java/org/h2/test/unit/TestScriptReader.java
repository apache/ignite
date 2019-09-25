/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.StringReader;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.util.ScriptReader;

/**
 * Tests the script reader tool that breaks up SQL scripts in statements.
 */
public class TestScriptReader extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() {
        testCommon();
        testRandom();
    }

    private void testRandom() {
        int len = getSize(1000, 10000);
        Random random = new Random(10);
        for (int i = 0; i < len; i++) {
            int l = random.nextInt(10);
            String[] sql = new String[l];
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < l; j++) {
                sql[j] = randomStatement(random);
                buff.append(sql[j]);
                if (j < l - 1) {
                    buff.append(";");
                }
            }
            String s = buff.toString();
            StringReader reader = new StringReader(s);
            try (ScriptReader source = new ScriptReader(reader)) {
                for (int j = 0; j < l; j++) {
                    String e = source.readStatement();
                    String c = sql[j];
                    if (c.length() == 0 && j == l - 1) {
                        c = null;
                    }
                    assertEquals(c, e);
                }
                assertEquals(null, source.readStatement());
            }
        }
    }

    private static String randomStatement(Random random) {
        StringBuilder buff = new StringBuilder();
        int len = random.nextInt(5);
        for (int i = 0; i < len; i++) {
            switch (random.nextInt(10)) {
            case 0: {
                int l = random.nextInt(4);
                String[] ch = { "\n", "\r", " ", "*", "a", "0", "$ " };
                for (int j = 0; j < l; j++) {
                    buff.append(ch[random.nextInt(ch.length)]);
                }
                break;
            }
            case 1: {
                buff.append('\'');
                int l = random.nextInt(4);
                String[] ch = { ";", "\n", "\r", "--", "//", "/", "-", "*",
                        "/*", "*/", "\"", "$ " };
                for (int j = 0; j < l; j++) {
                    buff.append(ch[random.nextInt(ch.length)]);
                }
                buff.append('\'');
                break;
            }
            case 2: {
                buff.append('"');
                int l = random.nextInt(4);
                String[] ch = { ";", "\n", "\r", "--", "//", "/", "-", "*",
                        "/*", "*/", "\'", "$" };
                for (int j = 0; j < l; j++) {
                    buff.append(ch[random.nextInt(ch.length)]);
                }
                buff.append('"');
                break;
            }
            case 3: {
                buff.append('-');
                if (random.nextBoolean()) {
                    String[] ch = { "\n", "\r", "*", "a", " ", "$ " };
                    int l = 1 + random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                } else {
                    buff.append('-');
                    String[] ch = { ";", "-", "//", "/*", "*/", "a", "$" };
                    int l = random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                    buff.append('\n');
                }
                break;
            }
            case 4: {
                buff.append('/');
                if (random.nextBoolean()) {
                    String[] ch = { "\n", "\r", "a", " ", "- ", "$ " };
                    int l = 1 + random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                } else {
                    buff.append('*');
                    String[] ch = { ";", "-", "//", "/* ", "--", "\n", "\r", "a", "$" };
                    int l = random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                    buff.append("*/");
                }
                break;
            }
            case 5: {
                if (buff.length() > 0) {
                    buff.append(" ");
                }
                buff.append("$");
                if (random.nextBoolean()) {
                    String[] ch = { "\n", "\r", "a", " ", "- ", "/ " };
                    int l = 1 + random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                } else {
                    buff.append("$");
                    String[] ch = { ";", "-", "//", "/* ", "--", "\n", "\r", "a", "$ " };
                    int l = random.nextInt(4);
                    for (int j = 0; j < l; j++) {
                        buff.append(ch[random.nextInt(ch.length)]);
                    }
                    buff.append("$$");
                }
                break;
            }
            default:
            }
        }
        return buff.toString();
    }

    private void testCommon() {
        String s;
        ScriptReader source;

        s = "$$;$$;";
        source = new ScriptReader(new StringReader(s));
        assertEquals("$$;$$", source.readStatement());
        assertEquals(null, source.readStatement());
        source.close();

        s = "a;';';\";\";--;\n;/*;\n*/;//;\na;";
        source = new ScriptReader(new StringReader(s));
        assertEquals("a", source.readStatement());
        assertEquals("';'", source.readStatement());
        assertEquals("\";\"", source.readStatement());
        assertEquals("--;\n", source.readStatement());
        assertEquals("/*;\n*/", source.readStatement());
        assertEquals("//;\na", source.readStatement());
        assertEquals(null, source.readStatement());
        source.close();

        s = "/\n$ \n\n $';$$a$$ $\n;'";
        source = new ScriptReader(new StringReader(s));
        assertEquals("/\n$ \n\n $';$$a$$ $\n;'", source.readStatement());
        assertEquals(null, source.readStatement());
        source.close();

        // check handling of unclosed block comments
        s = "/*xxx";
        source = new ScriptReader(new StringReader(s));
        assertEquals("/*xxx", source.readStatement());
        assertTrue(source.isBlockRemark());
        source.close();
    }

}
