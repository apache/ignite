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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.query.RunningFragmentInfo;
import org.apache.ignite.internal.processors.query.RunningQueryInfo;
import org.apache.ignite.internal.processors.query.RunningStage;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.util.Service;

/** */
public class RunningQueryService implements Service {
    /** */
    private final Map<UUID, RunningQueryInfo> queries;

    /** */
    private final Map<UUID, List<RunningFragmentInfo>> fragments;

    /** */
    public RunningQueryService() {
        queries = new ConcurrentHashMap<>();
        fragments = new ConcurrentHashMap<>();
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

        queries.put(run.qryId(), run);

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

        RunningQueryInfo prev = queries.put(run.qryId(), run);

        assert prev == null;

        return qryId;
    }

    /**
     * Deregister running query or their fragment.
     *
     * @param qryId id of the query, which is given by {@link #register register} method.
     */
    public void deregister(UUID qryId) {
        queries.remove(qryId);
        fragments.remove(qryId);
    }

    /**
     * Register running fragment of query
     *
     * @param qryId id of the query related to fragment,
     * which is given by {@link #register register} method on originator node.
     * @param pctx Planning context.
     */
    public synchronized void registerFragment(UUID qryId, long fragmentId, PlanningContext pctx) {
        RunningFragmentInfo fragmentInfo = new RunningFragmentInfo(
            qryId,
            pctx.query(),
            pctx.schemaName(),
            pctx.queryCancel(),
            pctx.originatingNodeId(),
            fragmentId);

        fragments.computeIfAbsent(qryId, k -> new ArrayList<>()).add(fragmentInfo);
    }

    /**
     * Deregister running fragment
     *
     * @param qryId id of the query, which is given by {@link #register register} method.
     * @param fragmentId if of fragment related to query id.
     */
    public synchronized void deregisterFragment(UUID qryId, long fragmentId) {
        List<RunningFragmentInfo> frs = fragments.get(qryId);
        if (frs != null && !frs.isEmpty()) {
            frs.removeIf(info -> info.fragmentId() == fragmentId);

            if (frs.isEmpty())
                fragments.remove(qryId);
        }
    }

    /**
     * Cancel query.
     *
     * @param qryId Query id.
     * @return {@code true} If query found and cancellation has been invoked.
     */
    public boolean cancelQuery(UUID qryId) {
        RunningQueryInfo runningInfo = queries.get(qryId);

        if (runningInfo != null) {
            runningInfo.cancel();

            return true;
        }

        List<RunningFragmentInfo> frs = new ArrayList<>(fragments.get(qryId));

        if (frs != null) {
            frs.forEach(RunningFragmentInfo::cancel);

            return true;
        }

        return false;
    }

    /**
     * Gets info about running query by their id.
     *
     * @param qryId Id of running query.
     * @return Running query info or {@code null} in case no running query for given id.
     */
    public RunningQueryInfo query(UUID qryId) {
        return queries.get(qryId);
    }



    /** {@inheritDoc} */
    @Override public void tearDown() {
        queries.clear();
        fragments.clear();
    }

    /**
     * Return SQL queries which executing right now.
     *
     * @return List of SQL running queries.
     */
    public List<RunningQueryInfo> runningQueries() {
        return new ArrayList<>(queries.values());
    }

    /**
     * Return SQL queries which executing right now.
     *
     * @return List of SQL running queries.
     */
    public List<RunningFragmentInfo> runningFragments() {
        return fragments.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }
}
