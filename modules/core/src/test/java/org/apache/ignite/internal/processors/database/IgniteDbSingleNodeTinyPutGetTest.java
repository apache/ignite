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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Test for
 */
public class IgniteDbSingleNodeTinyPutGetTest extends IgniteDbSingleNodePutGetTest {
    /** {@inheritDoc} */
    @Override protected boolean isLargePage() {
        return true;
    }

    /** */
    @Test
    public void testPutGetTiny() {
        IgniteEx ig = grid(0);

        IgniteCache<Short, Byte> cache = ig.cache("tiny");

        for (short i = 0; i < 1000; i++)
            cache.put(i, (byte) i);

        for (short i = 0; i < 1000; i++)
            assertEquals((byte) i, cache.get(i).byteValue());
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testGradualRandomPutAllRemoveAll() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRandomRemove() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRandomPut() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetSimple() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetLarge() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetOverwrite() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testOverwriteNormalSizeAfterSmallerSize() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutDoesNotTriggerRead() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetMultipleObjects() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testSizeClear() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testBounds() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testMultithreadedPut() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetRandomUniqueMultipleObjects() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutPrimaryUniqueSecondaryDuplicates() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetRandomNonUniqueMultipleObjects() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetRemoveMultipleForward() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRandomPutGetRemove() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutGetRemoveMultipleBackward() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIndexOverwrite() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testObjectKey() {
        // No-op
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIterators() {
        // No-op
    }
}
