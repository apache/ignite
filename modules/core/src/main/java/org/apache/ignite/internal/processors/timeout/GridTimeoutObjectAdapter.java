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

package org.apache.ignite.internal.processors.timeout;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Default implementation for {@link GridTimeoutObject}.
 */
public abstract class GridTimeoutObjectAdapter implements GridTimeoutObject {
    /** Timeout ID. */
    private final IgniteUuid id;

    /** End time. */
    private final long endTime;

    /**
     * @param timeout Timeout for this object.
     */
    protected GridTimeoutObjectAdapter(long timeout) {
        this(IgniteUuid.randomUuid(), timeout);
    }

    /**
     * @param id Timeout ID.
     * @param timeout Timeout for this object.
     */
    protected GridTimeoutObjectAdapter(IgniteUuid id, long timeout) {
        this.id = id;

        long endTime = timeout >= 0 ? U.currentTimeMillis() + timeout : Long.MAX_VALUE;

        this.endTime = endTime >= 0 ? endTime : Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }
}