/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;

import java.util.UUID;

/**
 * Query descriptor.
 */
public class GridRunningQueryInfo {
    /** */
    private final long id;

    /** Originating Node ID. */
    private final UUID nodeId;

    /** */
    private final String qry;

    /** Query type. */
    private final GridCacheQueryType qryType;

    /** Schema name. */
    private final String schemaName;

    /** */
    private final long startTime;

    /** */
    private final GridQueryCancel cancel;

    /** */
    private final boolean loc;

    /**
     * Constructor.
     *
     * @param id Query ID.
     * @param nodeId Originating node ID.
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param startTime Query start time.
     * @param cancel Query cancel.
     * @param loc Local query flag.
     */
    public GridRunningQueryInfo(
        Long id,
        UUID nodeId,
        String qry,
        GridCacheQueryType qryType,
        String schemaName,
        long startTime,
        GridQueryCancel cancel,
        boolean loc
    ) {
        this.id = id;
        this.nodeId = nodeId;
        this.qry = qry;
        this.qryType = qryType;
        this.schemaName = schemaName;
        this.startTime = startTime;
        this.cancel = cancel;
        this.loc = loc;
    }

    /**
     * @return Query ID.
     */
    public Long id() {
        return id;
    }

    /**
     * @return Global query ID.
     */
    public String globalQueryId() {
        return QueryUtils.globalQueryId(nodeId, id);
    }

    /**
     * @return Query text.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType queryType() {
        return qryType;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Query start time.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * @param curTime Current time.
     * @param duration Duration of long query.
     * @return {@code true} if this query should be considered as long running query.
     */
    public boolean longQuery(long curTime, long duration) {
        return curTime - startTime > duration;
    }

    /**
     * Cancel query.
     */
    public void cancel() {
        if (cancel != null)
            cancel.cancel();
    }

    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean cancelable() {
        return cancel != null;
    }

    /**
     * @return {@code true} if query is local.
     */
    public boolean local() {
        return loc;
    }
}
