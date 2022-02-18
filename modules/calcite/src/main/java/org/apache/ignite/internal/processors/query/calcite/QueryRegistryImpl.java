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
import java.util.stream.Collectors;
import org.apache.calcite.util.Pair;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.processors.query.RunningQueryManager;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Registry of the running queries.
 */
public class QueryRegistryImpl extends AbstractService implements QueryRegistry {
    /** */
    private final ConcurrentMap<UUID, Pair<Long, RunningQuery>> runningQrys = new ConcurrentHashMap<>();

    /** */
    protected final GridKernalContext kctx;

    /** */
    public QueryRegistryImpl(GridKernalContext ctx) {
        super(ctx);

        kctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public RunningQuery register(RunningQuery qry) {
        return runningQrys.computeIfAbsent(qry.id(), k -> {
            if (!(qry instanceof RootQuery))
                return Pair.of(RunningQueryManager.UNDEFINED_QUERY_ID, qry);

            RootQuery<?> rootQry = (RootQuery<?>)qry;

            RunningQueryManager qryMgr = kctx.query().runningQueryManager();

            long locId = qryMgr.register(rootQry.sql(), GridCacheQueryType.SQL_FIELDS, rootQry.context().schemaName(),
                false, createCancelToken(qry), kctx.localNodeId().toString());

            return Pair.of(locId, qry);
        }).right;
    }

    /** {@inheritDoc} */
    @Override public RunningQuery query(UUID id) {
        Pair<Long, RunningQuery> value = runningQrys.get(id);
        return value != null ? value.right : null;
    }

    /** {@inheritDoc} */
    @Override public void unregister(UUID id) {
        Pair<Long, RunningQuery> value = runningQrys.remove(id);
        if (value != null)
            kctx.query().runningQueryManager().unregister(value.left, null);
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends RunningQuery> runningQueries() {
        return runningQrys.values().stream().map(Pair::getValue).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        runningQrys.values().forEach(q -> IgniteUtils.close(q.right::cancel, log));
        runningQrys.clear();
    }

    /** */
    private static GridQueryCancel createCancelToken(RunningQuery qry) {
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
