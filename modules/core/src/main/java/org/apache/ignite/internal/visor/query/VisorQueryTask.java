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

package org.apache.ignite.internal.visor.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorEither;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.SQL_QRY_NAME;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.fetchSqlQueryRows;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.scheduleResultSetHolderRemoval;

/**
 * Task for execute SQL fields query and get first page of results.
 */
@GridInternal
public class VisorQueryTask extends VisorOneNodeTask<VisorQueryTaskArg, VisorEither<VisorQueryResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryJob job(VisorQueryTaskArg arg) {
        return new VisorQueryJob(arg, debug);
    }

    /**
     * Job for execute SCAN or SQL query and get first page of results.
     */
    private static class VisorQueryJob extends VisorJob<VisorQueryTaskArg, VisorEither<VisorQueryResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorQueryJob(VisorQueryTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorEither<VisorQueryResult> run(final VisorQueryTaskArg arg) {
            try {
                IgniteCache<Object, Object> c = ignite.context().cache().jcache(arg.getCacheName());
                UUID nid = ignite.localNode().id();

                SqlFieldsQuery qry = new SqlFieldsQuery(arg.getQueryText());
                qry.setPageSize(arg.getPageSize());
                qry.setLocal(arg.isLocal());
                qry.setDistributedJoins(arg.isDistributedJoins());
                qry.setEnforceJoinOrder(arg.isEnforceJoinOrder());
                qry.setReplicatedOnly(arg.isReplicatedOnly());

                long start = U.currentTimeMillis();

                VisorQueryCursor<List<?>> cur = new VisorQueryCursor<>(c.withKeepBinary().query(qry));

                Collection<GridQueryFieldMetadata> meta = cur.fieldsMeta();

                if (meta == null)
                    return new VisorEither<>(
                        new VisorExceptionWrapper(new SQLException("Fail to execute query. No metadata available.")));
                else {
                    List<VisorQueryField> names = new ArrayList<>(meta.size());

                    for (GridQueryFieldMetadata col : meta)
                        names.add(new VisorQueryField(col.schemaName(), col.typeName(),
                            col.fieldName(), col.fieldTypeName()));

                    List<Object[]> rows = fetchSqlQueryRows(cur, arg.getPageSize());

                    // Query duration + fetch duration.
                    long duration = U.currentTimeMillis() - start;

                    boolean hasNext = cur.hasNext();

                    // Generate query ID to store query cursor in node local storage.
                    String qryId = SQL_QRY_NAME + "-" + UUID.randomUUID();

                    if (hasNext) {
                        ignite.cluster().<String, VisorQueryCursor<List<?>>>nodeLocalMap().put(qryId, cur);

                        scheduleResultSetHolderRemoval(qryId, ignite);
                    }
                    else
                        cur.close();

                    return new VisorEither<>(new VisorQueryResult(nid, qryId, names, rows, hasNext, duration));
                }
            }
            catch (Throwable e) {
                return new VisorEither<>(new VisorExceptionWrapper(e));
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryJob.class, this);
        }
    }
}
