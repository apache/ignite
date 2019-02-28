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

/**
 * Kill Query run context.
 */
class KillQueryRun {
    /** Node id. */
    // TODO: Final
    private UUID nodeId;

    /** Node query id. */
    // TODO: Final
    private long nodeQryId;

    /**
     * Constructor.
     *
     * @param nodeId Node id.
     * @param nodeQryId Node query id.
     */
    public KillQueryRun(UUID nodeId, long nodeQryId) {
        assert nodeId != null;

        this.nodeQryId = nodeQryId;
        this.nodeId = nodeId;
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        KillQueryRun run = (KillQueryRun)o;

        if (nodeQryId != run.nodeQryId)
            return false;

        return nodeId.equals(run.nodeId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = nodeId.hashCode();

        result = 31 * result + (int)(nodeQryId ^ (nodeQryId >>> 32));

        return result;
    }
}
