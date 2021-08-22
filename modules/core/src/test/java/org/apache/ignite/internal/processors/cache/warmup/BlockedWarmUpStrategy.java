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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Warm-up strategy that only waits for {@link #stop} call.
 */
public class BlockedWarmUpStrategy implements WarmUpStrategy<BlockedWarmUpConfiguration> {
    /** Stop latch. */
    public final CountDownLatch stopLatch = new CountDownLatch(1);

    /** Start latch. */
    public final CountDownLatch startLatch = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override public Class<BlockedWarmUpConfiguration> configClass() {
        return BlockedWarmUpConfiguration.class;
    }

    /** {@inheritDoc} */
    @Override public void warmUp(
        BlockedWarmUpConfiguration cfg,
        DataRegion region
    ) throws IgniteCheckedException {
        startLatch.countDown();

        U.await(stopLatch);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        stopLatch.countDown();
    }
}
