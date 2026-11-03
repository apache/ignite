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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.Order;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.AbstractMessage;

/** Optional acknowledgement parameters for a distributed unlock. */
public class GridCacheUnlockRequestOptions extends AbstractMessage {
    /** Future awaiting cleanup. */
    @Order(0)
    public IgniteUuid futId;

    /** Remaining transaction timeout; zero means unlimited. */
    @Order(1)
    public long timeout;

    /** Required by the message factory. */
    public GridCacheUnlockRequestOptions() {
        // No-op.
    }

    /**
     * @param futId Future awaiting cleanup.
     * @param timeout Remaining transaction timeout.
     */
    public GridCacheUnlockRequestOptions(IgniteUuid futId, long timeout) {
        this.futId = futId;
        this.timeout = timeout;
    }
}
