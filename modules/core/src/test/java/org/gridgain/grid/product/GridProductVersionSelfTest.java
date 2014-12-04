/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.product;

import org.apache.ignite.product.*;
import org.gridgain.testframework.junits.common.*;

import static org.junit.Assert.*;
import static org.gridgain.grid.kernal.GridProductImpl.*;

/**
 * Versions test.
 */
public class GridProductVersionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testFromString() throws Exception {
        GridProductVersion ver = GridProductVersion.fromString("1.2.3");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = GridProductVersion.fromString("1.2.3-ent-0-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = GridProductVersion.fromString("1.2.3-rc1-os-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("rc1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = GridProductVersion.fromString("1.2.3-GA1-4-DEV");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("GA1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = GridProductVersion.fromString("1.2.3-ent");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(0, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = GridProductVersion.fromString("1.2.3-os-4");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[20], ver.revisionHash());

        ver = GridProductVersion.fromString("1.2.3-ent-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[] {24,-27,-89,-20,-98,50,2,18,106,105,-68,35,26,107,-106,91,-63,-41,61,-18},
            ver.revisionHash());

        ver = GridProductVersion.fromString("1.2.3-rc1-os-4-18e5a7ec9e3202126a69bc231a6b965bc1d73dee");

        assertEquals(1, ver.major());
        assertEquals(2, ver.minor());
        assertEquals(3, ver.maintenance());
        assertEquals("rc1", ver.stage());
        assertEquals(4, ver.revisionTimestamp());
        assertArrayEquals(new byte[] {24,-27,-89,-20,-98,50,2,18,106,105,-68,35,26,107,-106,91,-63,-41,61,-18},
            ver.revisionHash());

        GridProductVersion.fromString(VER + '-' + "ent" + '-' + BUILD_TSTAMP + '-' + REV_HASH);

        GridProductVersion.fromString(VER + '-' + "os" + '-' + BUILD_TSTAMP + '-' + REV_HASH);
    }
}
