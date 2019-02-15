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

package org.apache.ignite.internal.util.io;

import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_RECHECK;

/**
 * Test for {@link GridUnsafeDataOutput}.
 */
@RunWith(JUnit4.class)
public class GridUnsafeDataOutputArraySizingSelfTest extends GridCommonAbstractTest {
    /** Small array. */
    private static final byte[] SMALL = new byte[32];

    /** Big array. */
    private static final byte[] BIG = new byte[2048];

    /** Buffer timeout. */
    private static final long BUFFER_TIMEOUT = 1000;

    /** Wait timeout is bigger then buffer timeout to prevent failures due to time measurement error. */
    private static final long WAIT_BUFFER_TIMEOUT = BUFFER_TIMEOUT + BUFFER_TIMEOUT / 2;

    /**
     *
     */
    static {
        System.setProperty(IGNITE_MARSHAL_BUFFERS_RECHECK, Long.toString(BUFFER_TIMEOUT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSmall() throws Exception {
        final GridUnsafeDataOutput out = new GridUnsafeDataOutput(512);

        assertTrue(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 256), WAIT_BUFFER_TIMEOUT));
        assertTrue(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 128), WAIT_BUFFER_TIMEOUT));
        assertTrue(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 64), WAIT_BUFFER_TIMEOUT));
        assertFalse(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 32), WAIT_BUFFER_TIMEOUT));
        assertTrue(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 64), WAIT_BUFFER_TIMEOUT));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBig() throws Exception {
        GridUnsafeDataOutput out = new GridUnsafeDataOutput(512);

        out.write(BIG);
        out.reset();

        assertEquals(4096, out.internalArray().length);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    @Test
    public void testChanged1() throws Exception {
        GridUnsafeDataOutput out = new GridUnsafeDataOutput(512);

        for (int i = 0; i < 100; i++) {
            Thread.sleep(100);

            out.write(SMALL);
            out.reset();
            out.write(BIG);
            out.reset();
        }

        assertEquals(4096, out.internalArray().length);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testChanged2() throws Exception {
        final GridUnsafeDataOutput out = new GridUnsafeDataOutput(512);

        assertTrue(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 256), WAIT_BUFFER_TIMEOUT));

        out.write(BIG);
        out.reset();
        assertEquals(4096, out.internalArray().length);

        assertTrue(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 4096), WAIT_BUFFER_TIMEOUT));
        assertTrue(GridTestUtils.waitForCondition(new WriteAndCheckPredicate(out, SMALL, 2048), 2 * WAIT_BUFFER_TIMEOUT));
    }

    /**
     *
     */
    private static class WriteAndCheckPredicate implements GridAbsPredicate {
        /** */
        final GridUnsafeDataOutput out;

        /** */
        final byte [] bytes;

        /** */
        final int len;

        /**
         * @param out Out.
         * @param bytes Bytes.
         * @param len Length.
         */
        WriteAndCheckPredicate(GridUnsafeDataOutput out, byte[] bytes, int len) {
            this.out = out;
            this.bytes = bytes;
            this.len = len;
        }

        /** {@inheritDoc} */
        @Override public boolean apply() {
            try {
                out.write(bytes);
                out.reset();

                System.out.println("L=" + out.internalArray().length);

                return out.internalArray().length == len;
            }
            catch (Exception e) {
                assertTrue("Unexpected exception: " + e.getMessage(), false);
                return false;
            }
        }
    }
}
