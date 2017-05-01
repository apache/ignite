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

    /**
     * @throws Exception If fail.
     */
    public void testPutGetTiny() throws Exception {
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
    @Override public void testPutGetSimple() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetLarge() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetOverwrite() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testOverwriteNormalSizeAfterSmallerSize() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutDoesNotTriggerRead() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetMultipleObjects() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testSizeClear() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testBounds() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testMultithreadedPut() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRandomUniqueMultipleObjects() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutPrimaryUniqueSecondaryDuplicates() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRandomNonUniqueMultipleObjects() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRemoveMultipleForward() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void _testRandomPutGetRemove() {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testPutGetRemoveMultipleBackward() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testIndexOverwrite() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testObjectKey() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void testIterators() throws Exception {
        // No-op
    }
}
