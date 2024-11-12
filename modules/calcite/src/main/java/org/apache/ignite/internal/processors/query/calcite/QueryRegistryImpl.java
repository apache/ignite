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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.running.RunningQueryManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Registry of the running queries.
 */
public class QueryRegistryImpl extends AbstractService implements QueryRegistry {
    /** */
    private final ConcurrentMap<UUID, Query<?>> runningQrys = new ConcurrentHashMap<>();

    /** */
    protected final GridKernalContext kctx;

    /** */
    public QueryRegistryImpl(GridKernalContext ctx) {
        super(ctx);

        kctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Query<?> register(Query<?> qry) {
        return runningQrys.computeIfAbsent(qry.id(), k -> {
            if (!(qry instanceof RootQuery))
                return qry;

            RootQuery<?> rootQry = (RootQuery<?>)qry;

            RunningQueryManager qryMgr = kctx.query().runningQueryManager();

            SqlFieldsQuery fieldsQry = rootQry.context().unwrap(SqlFieldsQuery.class);

            String initiatorId = fieldsQry != null ? fieldsQry.getQueryInitiatorId() : null;

            long locId = qryMgr.register(rootQry.sql(), GridCacheQueryType.SQL_FIELDS, rootQry.context().schemaName(),
                false, createCancelToken(qry), initiatorId, false, true, false);

            rootQry.localQueryId(locId);

            qryMgr.heavyQueriesTracker().startTracking(rootQry);

            return qry;
        });
    }

    /** {@inheritDoc} */
    @Override public Query<?> query(UUID id) {
        return runningQrys.get(id);
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID id, @Nullable Throwable failReason) {
        Query<?> val = runningQrys.remove(id);
        if (val instanceof RootQuery<?>) {
            RootQuery<?> qry = (RootQuery<?>)val;
            kctx.query().runningQueryManager().unregister(qry.localQueryId(), failReason);
            kctx.query().runningQueryManager().heavyQueriesTracker().stopTracking(qry, failReason);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends Query<?>> runningQueries() {
        return runningQrys.values();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        runningQrys.values().forEach(q -> IgniteUtils.close(q::cancel, log));
        runningQrys.clear();
    }

    /** */
    private static GridQueryCancel createCancelToken(Query<?> qry) {
        GridQueryCancel token = new GridQueryCancel();
        try {
            token.add(qry::cancel);
        }
        catch (QueryCancelledException ignore) {
            // Ignore, since it is impossible;
        }
        return token;
    }
}
