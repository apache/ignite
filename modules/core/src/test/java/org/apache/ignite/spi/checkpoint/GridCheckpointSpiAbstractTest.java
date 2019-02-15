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

package org.apache.ignite.spi.checkpoint;

import org.apache.ignite.GridTestIoUtils;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.junit.Test;

/**
 * Grid checkpoint SPI abstract test.
 * @param <T> Concrete SPI class.
 */
public abstract class GridCheckpointSpiAbstractTest<T extends CheckpointSpi>
    extends GridSpiAbstractTest<T> {
    /** */
    private static final int CHECK_POINT_COUNT = 10;

    /** */
    private static final String CHECK_POINT_KEY_PREFIX = "testCheckpoint";

    /**
     * @throws Exception Thrown in case of any errors.
     */
    @Test
    public void testSaveLoadRemoveWithoutExpire() throws Exception {
        String dataPrefix = "Test check point data ";

        // Save states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            GridCheckpointTestState state = new GridCheckpointTestState(dataPrefix + i);

            getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX + i, GridTestIoUtils.serializeJdk(state), 0, true);

            info("Saved check point [key=" + CHECK_POINT_KEY_PREFIX + i + ", data=" + state + ']');
        }

        // Load and check states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            byte[] serState = getSpi().loadCheckpoint(CHECK_POINT_KEY_PREFIX + i);

            assertNotNull("Missing checkpoint: " + CHECK_POINT_KEY_PREFIX + i, serState);

            GridCheckpointTestState state = GridTestIoUtils.deserializeJdk(serState);

            info("Loaded check point [key=" + CHECK_POINT_KEY_PREFIX + i + ", data=" + state + ']');

            assert state != null : "Can't load checkpoint state for key: " + CHECK_POINT_KEY_PREFIX + i;
            assert (dataPrefix + i).equals(state.getData()) : "Invalid state loaded [expected='" +
                dataPrefix + i + "', received='" + state.getData() + "']";
        }

        // Remove states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            boolean rmv = getSpi().removeCheckpoint(CHECK_POINT_KEY_PREFIX + i);

            if (rmv)
                info("Removed check point: " + CHECK_POINT_KEY_PREFIX + i);
            else
                info("Can't remove check point: " + CHECK_POINT_KEY_PREFIX + i);
        }

        // Check that states was removed.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = CHECK_POINT_KEY_PREFIX + i;

            final byte[] serState = getSpi().loadCheckpoint(key);

            assertNull("Checkpoint state should not be loaded with key: " + key, serState);
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    @Test
    public void testSaveWithExpire() throws Exception {
        // Save states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            GridCheckpointTestState state = new GridCheckpointTestState("Test check point data " + i + '.');

            getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX + i, GridTestIoUtils.serializeJdk(state), 1, true);

            info("Saved check point [key=" + CHECK_POINT_KEY_PREFIX + i + ", data=" + state + ']');
        }

        // For small expiration intervals no warranty that state will be removed.
        Thread.sleep(100);

        // Check that states was removed.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = CHECK_POINT_KEY_PREFIX + i;

            final byte[] serState = getSpi().loadCheckpoint(key);

            assert serState == null : "Checkpoint state should not be loaded with key: " + key;
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    @Test
    public void testDuplicates() throws Exception {
        int idx1 = 1;
        int idx2 = 2;

        GridCheckpointTestState state1 = new GridCheckpointTestState(Integer.toString(idx1));
        GridCheckpointTestState state2 = new GridCheckpointTestState(Integer.toString(idx2));

        getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX, GridTestIoUtils.serializeJdk(state1), 0, true);
        getSpi().saveCheckpoint(CHECK_POINT_KEY_PREFIX, GridTestIoUtils.serializeJdk(state2), 0, true);

        byte[] serState = getSpi().loadCheckpoint(CHECK_POINT_KEY_PREFIX);

        GridCheckpointTestState state = GridTestIoUtils.deserializeJdk(serState);

        assert state != null;
        assert state.equals(state2) : "Unexpected checkpoint state.";

        // Remove.
        getSpi().removeCheckpoint(CHECK_POINT_KEY_PREFIX);

        serState = getSpi().loadCheckpoint(CHECK_POINT_KEY_PREFIX);

        final byte[] finalSerState = serState;

        assert finalSerState == null : "Checkpoint state should not be loaded with key: " + CHECK_POINT_KEY_PREFIX;
    }
}
