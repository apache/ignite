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
