/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.zk.internal;

import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;

/**
 *
 */
abstract class ZkTimeoutObject implements IgniteSpiTimeoutObject {
    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private final long endTime;

    /** */
    volatile boolean cancelled;

    /**
     * @param timeout Timeout.
     */
    ZkTimeoutObject(long timeout) {
        long endTime = timeout >= 0 ? System.currentTimeMillis() + timeout : Long.MAX_VALUE;

        this.endTime = endTime >= 0 ? endTime : Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public final IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public final long endTime() {
        return endTime;
    }
}
