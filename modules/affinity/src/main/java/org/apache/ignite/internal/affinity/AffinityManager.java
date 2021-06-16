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

package org.apache.ignite.internal.affinity;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.event.AffinityEvent;
import org.apache.ignite.internal.affinity.event.AffinityEventParameters;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Conditions;
import org.apache.ignite.internal.metastorage.client.EntryEvent;
import org.apache.ignite.internal.metastorage.client.Operations;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * Affinity manager is responsible for affinity function related logic including calculating affinity assignments.
 */
public class AffinityManager extends Producer<AffinityEvent, AffinityEventParameters> {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(AffinityManager.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.assignment.";

    /**
     * MetaStorage manager in order to watch private distributed affinity specific configuration, cause
     * ConfigurationManger handles only public configuration.
     */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager in order to handle and listen affinity specific configuration. */
    private final ConfigurationManager configurationMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /**
     * Creates a new affinity manager.
     *
     * @param configurationMgr Configuration module.
     * @param metaStorageMgr Meta storage service.
     * @param baselineMgr Baseline manager.
     */
    public AffinityManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        BaselineManager baselineMgr
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.baselineMgr = baselineMgr;

        metaStorageMgr.registerWatchByPrefix(new ByteArray(INTERNAL_PREFIX), new WatchListener() {
            @Override public boolean onUpdate(@NotNull WatchEvent watchEvt) {
                for (EntryEvent evt : watchEvt.entryEvents()) {
                    String keyAsString = new ByteArray(evt.newEntry().key().bytes()).toString();

                    String tabIdVal = keyAsString.substring(INTERNAL_PREFIX.length());

                    UUID tblId = UUID.fromString(tabIdVal);

                    if (evt.newEntry().value() == null) {
                        assert evt.oldEntry().value() != null : "Previous assignment is unknown";

                        List<List<ClusterNode>> assignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(
                            evt.oldEntry().value());

                        onEvent(AffinityEvent.REMOVED, new AffinityEventParameters(tblId, assignment), null);
                    }
                    else {
                        List<List<ClusterNode>> assignment = (List<List<ClusterNode>>)ByteUtils.fromBytes(
                            evt.newEntry().value());

                        onEvent(AffinityEvent.CALCULATED, new AffinityEventParameters(tblId, assignment), null);
                    }
                }

                return true;
            }

            @Override public void onError(@NotNull Throwable e) {
                LOG.error("Meta storage listener issue", e);
            }
        });
    }

    /**
     * Calculates an assignment for a table which was specified by id.
     *
     * @param tblId Table identifier.
     * @param tblName Table name.
     * @return A future which will complete when the assignment is calculated.
     */
    public CompletableFuture<Boolean> calculateAssignments(UUID tblId, String tblName) {
        TableConfiguration tblConfig = configurationMgr.configurationRegistry()
            .getConfiguration(TablesConfiguration.KEY).tables().get(tblName);

        var key = new ByteArray(INTERNAL_PREFIX + tblId);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-14716 Need to support baseline changes.
        return metaStorageMgr.invoke(
            Conditions.notExists(key),
            Operations.put(key, ByteUtils.toBytes(
                RendezvousAffinityFunction.assignPartitions(
                    baselineMgr.nodes(),
                    tblConfig.partitions().value(),
                    tblConfig.replicas().value(),
                    false,
                    null
                ))),
            Operations.noop());
    }

    /**
     * Removes an assignment for a table which was specified by id.
     *
     * @param tblId Table identifier.
     * @return A future which will complete when assignment is removed.
     */
    public CompletableFuture<Boolean> removeAssignment(UUID tblId) {
        var key = new ByteArray(INTERNAL_PREFIX + tblId);

        return metaStorageMgr.invoke(
            Conditions.exists(key),
            Operations.remove(key),
            Operations.noop());
    }
}
