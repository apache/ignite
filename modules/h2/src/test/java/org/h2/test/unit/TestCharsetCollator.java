/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.text.Collator;
import org.h2.test.TestBase;
import org.h2.value.CharsetCollator;
import org.h2.value.CompareMode;

/**
 * Unittest for org.h2.value.CharsetCollator
 */
public class TestCharsetCollator extends TestBase {
    private CharsetCollator cp500Collator = new CharsetCollator(Charset.forName("cp500"));
    private CharsetCollator utf8Collator = new CharsetCollator(StandardCharsets.UTF_8);

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
        testBasicComparison();
        testNumberToCharacterComparison();
        testLengthComparison();
        testCreationFromCompareMode();
        testCreationFromCompareModeWithInvalidCharset();
    }

    private void testCreationFromCompareModeWithInvalidCharset() {
        try {
            CompareMode.getCollator("CHARSET_INVALID");
            fail();
        } catch (UnsupportedCharsetException e) {
            // expected
        }
    }

    private void testCreationFromCompareMode() {
        Collator utf8Col = CompareMode.getCollator("CHARSET_UTF-8");
        assertTrue(utf8Col instanceof CharsetCollator);
        assertEquals(((CharsetCollator) utf8Col).getCharset(), StandardCharsets.UTF_8);
    }

    private void testBasicComparison() {
        assertTrue(cp500Collator.compare("A", "B") < 0);
        assertTrue(cp500Collator.compare("AA", "AB") < 0);
    }

    private void testLengthComparison() {
        assertTrue(utf8Collator.compare("AA", "A") > 0);
    }

    private void testNumberToCharacterComparison() {
        assertTrue(cp500Collator.compare("A", "1") < 0);
        assertTrue(utf8Collator.compare("A", "1") > 0);
    }
}
