/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * SqlListenerUtils translations tests.
 */
public class SqlListenerUtilsTest {
    /**
     * Test pattern without any wildcard/wildchar.
     */
    @Test
    public void translateSimple() {
        check("some", "some");
        check("som\\\\e", "som\\e");
        check("some\\?", "some?");
    }

    /**
     * Test patterns with wildchar (include escaping).
     */
    @Test
    public void translateWildchar() {
        check(".", "_");
        check("som.", "som_");
        check("so.e", "so_e");
        check("so.eso.e", "so_eso_e");
        check(".ome", "_ome");

        check("so_e", "so\\_e");
    }

    /**
     * Test pattern with series of backslashes.
     */
    @Test
    public void translateSeriesOfBackslashes() {
        check("some_table", "some\\_table");
        check("some\\\\\\\\.table", "some\\\\_table");
        check("some\\\\\\\\_table", "some\\\\\\_table");
        check("some\\\\\\\\\\\\\\\\.table", "some\\\\\\\\_table");
        check("some\\\\\\\\\\\\\\\\_table", "some\\\\\\\\\\_table");

        check("some%table", "some\\%table");
        check("some\\\\\\\\.*table", "some\\\\%table");
        check("some\\\\\\\\%table", "some\\\\\\%table");
        check("some\\\\\\\\\\\\\\\\.*table", "some\\\\\\\\%table");
        check("some\\\\\\\\\\\\\\\\%table", "some\\\\\\\\\\%table");
    }

    /**
     * Test patterns with wildcard (include escaping).
     */
    @Test
    public void translateWildcard() {
        check(".*", "%");
        check("some.*", "some%");
        check("so.*e", "so%e");
        check("so.*eso.*e", "so%eso%e");
        check(".*ome", "%ome");

        check("so%e", "so\\%e");
    }

    /**
     * Test pattern that contains all printable characters.
     */
    @Test
    public void translateAllPrintableCharacters() {
        String str = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";

        String translated = SqlListenerUtils.translateSqlWildcardsToRegex(str);

        assertTrue(str.matches(translated));
    }

    /**
     * Check sql regex translation logic.
     *
     * @param exp Expected result.
     * @param sqlPtrn SQL regex pattern.
     */
    private void check(String exp, String sqlPtrn) {
        String actualRes = SqlListenerUtils.translateSqlWildcardsToRegex(sqlPtrn);

        assertEquals(exp, actualRes);
    }
}
