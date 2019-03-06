/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.product;

import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteVersionUtils.BUILD_TSTAMP;
import static org.apache.ignite.internal.IgniteVersionUtils.REV_HASH_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.junit.Assert.assertArrayEquals;

/**
 * Versions test.
 */
public class GridProductVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
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

        ver = IgniteProductVersion.fromString("1.2.3.b1-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("b1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3.final-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("final", ver.stage());
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

        ver = IgniteProductVersion.fromString("1.2.3.b1-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("b1", ver.stage());
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

        ver = IgniteProductVersion.fromString("1.2.3-SNAPSHOT-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("SNAPSHOT", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        ver = IgniteProductVersion.fromString("1.2.3.b1-SNAPSHOT-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("b1-SNAPSHOT", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[]{24, -27, -89, -20, -98, 50, 2, 18, 106, 105, -68, 35, 26, 107, -106, 91, -63, -41, 61, -18},
            ver.revisionHash());

        IgniteProductVersion.fromString(VER_STR + '-' + BUILD_TSTAMP + '-' + REV_HASH_STR);
    }
}
