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

package org.apache.ignite.internal.product;

import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.internal.IgniteVersionUtils.*;
import static org.junit.Assert.*;

/**
 * Versions test.
 */
public class GridProductVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testFromString() throws Exception {
        IgniteProductVersion ver = IgniteProductVersion.fromString("1.2.3");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-0-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-rc1-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("rc1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-GA1-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("GA1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-4");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3-rc1-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("rc1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        IgniteProductVersion.fromString(VER_STR + '-' + BUILD_TSTAMP + '-' + REV_HASH_STR);
    }
}
