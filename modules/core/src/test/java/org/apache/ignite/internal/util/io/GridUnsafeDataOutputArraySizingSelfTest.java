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

package org.apache.ignite.internal.util.io;

import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_RECHECK;

/**
 * Test for {@link GridUnsafeDataOutput}.
 */
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
        final byte[] bytes;

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
