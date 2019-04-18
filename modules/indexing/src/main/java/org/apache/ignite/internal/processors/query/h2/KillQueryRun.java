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
 *
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.UUID;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Kill Query run context.
 */
class KillQueryRun {
    /** Node id. */
    private final UUID nodeId;

    /** Node query id. */
    private final long nodeQryId;

    /** Cancellation query future. */
    private final GridFutureAdapter<String> cancelFut;

    /**
     * Constructor.
     *
     * @param nodeId Node id.
     * @param cancelFut Cancellation query future.
     */
    public KillQueryRun(UUID nodeId, long nodeQryId, GridFutureAdapter<String> cancelFut) {
        assert nodeId != null;

        this.nodeId = nodeId;
        this.nodeQryId = nodeQryId;
        this.cancelFut = cancelFut;
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Node query id.
     */
    public long nodeQryId() {
        return nodeQryId;
    }

    /**
     * @return Cancellation query future.
     */
    public GridFutureAdapter<String> cancelFuture() {
        return cancelFut;
    }
}
