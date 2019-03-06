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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

import java.util.concurrent.TimeUnit;

/**
 * Sql query request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheSqlQueryRequest extends ClientCacheRequest {
    /** Query. */
    private final SqlQuery qry;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheSqlQueryRequest(BinaryRawReaderEx reader) {
        super(reader);

        qry = new SqlQuery(reader.readString(), reader.readString())
                .setArgs(PlatformCache.readQueryArgs(reader))
                .setDistributedJoins(reader.readBoolean())
                .setLocal(reader.readBoolean())
                .setReplicatedOnly(reader.readBoolean())
                .setPageSize(reader.readInt())
                .setTimeout((int) reader.readLong(), TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache cache = cache(ctx);

        ctx.incrementCursors();

        try {
            QueryCursor cur = cache.query(qry);

            ClientCacheEntryQueryCursor cliCur = new ClientCacheEntryQueryCursor(
                    cur, qry.getPageSize(), ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            return new ClientCacheQueryResponse(requestId(), cliCur);
        }
        catch (Exception e) {
            ctx.decrementCursors();

            throw e;
        }
    }
}
