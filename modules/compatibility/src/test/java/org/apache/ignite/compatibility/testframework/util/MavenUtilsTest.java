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

package org.apache.ignite.compatibility.testframework.util;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.compatibility.testframework.util.MavenUtils.escapeSpaceCharsInPath;

/**
 * Test class for {@link MavenUtils}.
 */
public class MavenUtilsTest {
    /** Remains the same. */
    private static final String NO_NEED_TO_ESCAPE_PATH_0 = "C:\\maven\\mvn.bat clean install";

    /** Remains the same. */
    private static final String NO_NEED_TO_ESCAPE_PATH_1 = "C:\\mvn.bat clean install";

    /** Space chars in the middle of the path require escaping. */
    private static final String UNESCAPED_PATH_0 = "C:\\software\\Apache Software Foundation\\maven\\bin\\mvn.bat dependency:tree";

    /** Path with space chars escaped. */
    private static final String ESCAPED_PATH_0 = "C:\\software\\\"Apache Software Foundation\"\\maven\\bin\\mvn.bat dependency:tree";

    /** Space chars in several elements of the path require escaping. */
    private static final String UNESCAPED_PATH_1 = "C:\\Program Files\\Apache Software Foundation\\maven\\bin\\mvn.bat clean install";

    /** Path with space chars escaped. */
    private static final String ESCAPED_PATH_1 = "C:\\\"Program Files\"\\\"Apache Software Foundation\"\\maven\\bin\\mvn.bat clean install";

    /** */
    @Test
    public void testPathsWithoutSpacesStayUnchanged() {
        Assert.assertEquals(NO_NEED_TO_ESCAPE_PATH_0, escapeSpaceCharsInPath(NO_NEED_TO_ESCAPE_PATH_0));

        Assert.assertEquals(NO_NEED_TO_ESCAPE_PATH_1, escapeSpaceCharsInPath(NO_NEED_TO_ESCAPE_PATH_1));
    }

    /** */
    @Test
    public void testPathsWithSpaceCharsEscaped() {
        Assert.assertEquals(ESCAPED_PATH_0, escapeSpaceCharsInPath(UNESCAPED_PATH_0));

        Assert.assertEquals(ESCAPED_PATH_1, escapeSpaceCharsInPath(UNESCAPED_PATH_1));
    }
}
