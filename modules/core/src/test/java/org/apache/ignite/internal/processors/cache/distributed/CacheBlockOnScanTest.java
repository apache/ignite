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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Objects;
import java.util.Random;
import org.apache.ignite.cache.query.ScanQuery;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class CacheBlockOnScanTest extends CacheBlockOnReadAbstractTest {

    /** {@inheritDoc} */
    @Override @NotNull protected CacheReadBackgroundOperation<?, ?> getReadOperation() {
        return new IntCacheReadBackgroundOperation() {
            /** Random. */
            private Random random = new Random();

            /** {@inheritDoc} */
            @Override public void doRead() {
                int idx = random.nextInt(entriesCount());

                cache().query(new ScanQuery<>((k, v) -> Objects.equals(k, idx))).getAll();
            }
        };
    }

    /** {@inheritDoc} */
    @Params(baseline = 9, atomicityMode = ATOMIC, cacheMode = PARTITIONED, allowException = true)
    @Override public void testStopBaselineAtomicPartitioned() throws Exception {
        super.testStopBaselineAtomicPartitioned();
    }

    /** {@inheritDoc} */
    @Params(baseline = 9, atomicityMode = ATOMIC, cacheMode = REPLICATED, allowException = true)
    @Override public void testStopBaselineAtomicReplicated() throws Exception {
        super.testStopBaselineAtomicReplicated();
    }

    /** {@inheritDoc} */
    @Params(baseline = 9, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED, allowException = true)
    @Override public void testStopBaselineTransactionalPartitioned() throws Exception {
        super.testStopBaselineTransactionalPartitioned();
    }

    /** {@inheritDoc} */
    @Params(baseline = 9, atomicityMode = TRANSACTIONAL, cacheMode = REPLICATED, allowException = true)
    @Override public void testStopBaselineTransactionalReplicated() throws Exception {
        super.testStopBaselineTransactionalReplicated();
    }
}
