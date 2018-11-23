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

/**
 * Test for
 */
public class IgniteDbSingleNodeTinyPutGetTest extends IgniteDbSingleNodePutGetTest {
    /** {@inheritDoc} */
    @Override protected boolean isLargePage() {
        return true;
    }

    /** */
    public void testPutGetTiny() {
        IgniteEx ig = grid(0);

        IgniteCache<Short, Byte> cache = ig.cache("tiny");

        for (short i = 0; i < 1000; i++)
            cache.put(i, (byte) i);

        for (short i = 0; i < 1000; i++)
            assertEquals((byte) i, cache.get(i).byteValue());
    }

    /** {@inheritDoc} */
    @Override public void testGradualRandomPutAllRemoveAll() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testRandomRemove() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testRandomPut() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetSimple() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetLarge() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetOverwrite() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testOverwriteNormalSizeAfterSmallerSize() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutDoesNotTriggerRead() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetMultipleObjects() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testSizeClear() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testBounds() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testMultithreadedPut() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRandomUniqueMultipleObjects() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutPrimaryUniqueSecondaryDuplicates() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRandomNonUniqueMultipleObjects() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRemoveMultipleForward() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testRandomPutGetRemove() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRemoveMultipleBackward() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testIndexOverwrite() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testObjectKey() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testIterators() {
        // No-op
    }
}
