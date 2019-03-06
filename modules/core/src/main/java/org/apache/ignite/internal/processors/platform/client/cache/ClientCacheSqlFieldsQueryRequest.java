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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcStatementType;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;

/**
 * Sql query request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheSqlFieldsQueryRequest extends ClientCacheRequest {
    /** Query. */
    private final SqlFieldsQuery qry;

    /** Include field names flag. */
    private final boolean includeFieldNames;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    public ClientCacheSqlFieldsQueryRequest(BinaryRawReaderEx reader) {
        super(reader);

        // Same request format as in JdbcQueryExecuteRequest.
        String schema = reader.readString();
        int pageSize = reader.readInt();
        reader.readInt();  // maxRows
        String sql = reader.readString();
        Object[] args = PlatformCache.readQueryArgs(reader);
        JdbcStatementType stmtType = JdbcStatementType.fromOrdinal(reader.readByte());
        boolean distributedJoins = reader.readBoolean();
        boolean loc = reader.readBoolean();
        boolean replicatedOnly = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean collocated = reader.readBoolean();
        boolean lazy = reader.readBoolean();
        int timeout = (int) reader.readLong();
        includeFieldNames = reader.readBoolean();

        SqlFieldsQuery qry = stmtType == JdbcStatementType.ANY_STATEMENT_TYPE
                ? new SqlFieldsQuery(sql)
                : new SqlFieldsQueryEx(sql,stmtType == JdbcStatementType.SELECT_STATEMENT_TYPE);

        qry.setSchema(schema)
                .setPageSize(pageSize)
                .setArgs(args)
                .setDistributedJoins(distributedJoins)
                .setLocal(loc)
                .setReplicatedOnly(replicatedOnly)
                .setEnforceJoinOrder(enforceJoinOrder)
                .setCollocated(collocated)
                .setLazy(lazy)
                .setTimeout(timeout, TimeUnit.MILLISECONDS);

        this.qry = qry;
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ctx.incrementCursors();

        try {
            // If cacheId is provided, we must check the cache for existence.
            if (cacheId() != 0) {
                DynamicCacheDescriptor desc = cacheDescriptor(ctx);

                if (qry.getSchema() == null) {
                    String schema = QueryUtils.normalizeSchemaName(desc.cacheName(),
                        desc.cacheConfiguration().getSqlSchema());

                    qry.setSchema(schema);
                }
            }

            List<FieldsQueryCursor<List<?>>> curs = ctx.kernalContext().query().querySqlFields(qry, true, true);

            assert curs.size() == 1;

            FieldsQueryCursor cur = curs.get(0);

            ClientCacheFieldsQueryCursor cliCur = new ClientCacheFieldsQueryCursor(
                cur, qry.getPageSize(), ctx);

            long cursorId = ctx.resources().put(cliCur);

            cliCur.id(cursorId);

            return new ClientCacheSqlFieldsQueryResponse(requestId(), cliCur, cur, includeFieldNames);
        }
        catch (Exception e) {
            ctx.decrementCursors();

            SecurityException securityEx = X.cause(e, SecurityException.class);

            if (securityEx != null) {
                throw new IgniteClientException(
                    ClientStatus.SECURITY_VIOLATION,
                    "Client is not authorized to perform this operation",
                    securityEx
                );
            }

            throw e;
        }
    }
}
