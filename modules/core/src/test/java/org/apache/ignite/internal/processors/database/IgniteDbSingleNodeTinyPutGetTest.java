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
