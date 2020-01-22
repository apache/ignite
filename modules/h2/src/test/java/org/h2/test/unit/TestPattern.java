/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.text.Collator;
import org.h2.expression.CompareLike;
import org.h2.test.TestBase;
import org.h2.value.CompareMode;

/**
 * Tests LIKE pattern matching.
 */
public class TestPattern extends TestBase {

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
        testCompareModeReuse();
        testPattern();
    }

    private void testCompareModeReuse() {
        CompareMode mode1, mode2;
        mode1 = CompareMode.getInstance(null, 0);
        mode2 = CompareMode.getInstance(null, 0);
        assertTrue(mode1 == mode2);

        mode1 = CompareMode.getInstance("DE", Collator.SECONDARY);
        assertFalse(mode1 == mode2);
        mode2 = CompareMode.getInstance("DE", Collator.SECONDARY);
        assertTrue(mode1 == mode2);
    }

    private void testPattern() {
        CompareMode mode = CompareMode.getInstance(null, 0);
        CompareLike comp = new CompareLike(mode, "\\", null, null, null, false);
        test(comp, "B", "%_");
        test(comp, "A", "A%");
        test(comp, "A", "A%%");
        test(comp, "A_A", "%\\_%");

        for (int i = 0; i < 10000; i++) {
            String pattern = getRandomPattern();
            String value = getRandomValue();
            test(comp, value, pattern);
        }
    }

    private void test(CompareLike comp, String value, String pattern) {
        String regexp = initPatternRegexp(pattern, '\\');
        boolean resultRegexp = value.matches(regexp);
        boolean result = comp.test(pattern, value, '\\');
        if (result != resultRegexp) {
            fail("Error: >" + value + "< LIKE >" + pattern + "< result=" +
                    result + " resultReg=" + resultRegexp);
        }
    }

    private static String getRandomValue() {
        StringBuilder buff = new StringBuilder();
        int len = (int) (Math.random() * 10);
        String s = "AB_%\\";
        for (int i = 0; i < len; i++) {
            buff.append(s.charAt((int) (Math.random() * s.length())));
        }
        return buff.toString();
    }

    private static String getRandomPattern() {
        StringBuilder buff = new StringBuilder();
        int len = (int) (Math.random() * 4);
        String s = "A%_\\";
        for (int i = 0; i < len; i++) {
            char c = s.charAt((int) (Math.random() * s.length()));
            if ((c == '_' || c == '%') && Math.random() > 0.5) {
                buff.append('\\');
            } else if (c == '\\') {
                buff.append(c);
            }
            buff.append(c);
        }
        return buff.toString();
    }

    private String initPatternRegexp(String pattern, char escape) {
        int len = pattern.length();
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < len; i++) {
            char c = pattern.charAt(i);
            if (escape == c) {
                if (i >= len) {
                    fail("escape can't be last char");
                }
                c = pattern.charAt(++i);
                buff.append('\\');
                buff.append(c);
            } else if (c == '%') {
                buff.append(".*");
            } else if (c == '_') {
                buff.append('.');
            } else if (c == '\\') {
                buff.append("\\\\");
            } else {
                buff.append(c);
            }
            // TODO regexp: there are other chars that need escaping
        }
        String regexp = buff.toString();
        // System.out.println("regexp = " + regexp);
        return regexp;
    }

}
