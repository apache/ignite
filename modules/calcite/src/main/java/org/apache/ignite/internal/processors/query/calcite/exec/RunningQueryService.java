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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ignite.internal.processors.query.RunningQueryInfo;
import org.apache.ignite.internal.processors.query.RunningStage;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.util.Service;

/** */
public class RunningQueryService implements Service {
    /** */
    private final Map<UUID, RunningQueryInfo> running;

    /** */
    public RunningQueryService() {
        running = new ConcurrentHashMap<>();
    }

    /**
     * Register running query on planing stage.
     *
     * @param pctx Planning context.
     * @return Id of registered query.
     */
    public UUID register(PlanningContext pctx) {
        UUID qryId = UUID.randomUUID();

        RunningQueryInfo run = new RunningQueryInfo(
            RunningStage.PLANNING,
            qryId,
            pctx.query(),
            pctx.schemaName(),
            pctx.queryCancel());

        running.put(run.qryId(), run);

        return qryId;
    }

    /**
     * Register running query on execution stage.
     *
     * @param qry Query text.
     * @param pctx Planning context.
     * @return Id of registered query.
     */
    public UUID register(String qry, PlanningContext pctx) {
        UUID qryId = UUID.randomUUID();

        RunningQueryInfo run = new RunningQueryInfo(
            RunningStage.EXECUTION,
            qryId,
            qry,
            pctx.schemaName(),
            pctx.queryCancel());

        running.put(run.qryId(), run);

        return qryId;
    }

    /**
     * Unregister running query.
     *
     * @param qryId id of the query, which is given by {@link #register register} method.
     */
    public void unregister(UUID qryId) {
        running.remove(qryId);
    }

    /**
     * Cancel query.
     *
     * @param qryId Query id.
     */
    public void cancelQuery(UUID qryId) {
        RunningQueryInfo runningInfo = running.get(qryId);

        if (runningInfo != null)
            runningInfo.cancel();
    }

    /**
     * Gets info about running query by their id.
     * @param qryId Id of running query.
     * @return Running query info or {@code null} in case no running query for given id.
     */
    public RunningQueryInfo runningQueryInfo(UUID qryId) {
        return running.get(qryId);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() {
        running.clear();
    }

    /**
     * Return SQL queries which executing right now.
     *
     * @return List of SQL running queries.
     */
    public List<RunningQueryInfo> runningSqlQueries() {
        List<RunningQueryInfo> res = new ArrayList<>();

        for (RunningQueryInfo run : running.values()) {
                res.add(run);
        }

        return res;
    }
}
