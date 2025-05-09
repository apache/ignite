/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.web.socket;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.internal.util.lang.ClusterNodeFunc;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.stereotype.Repository;

import static java.util.stream.StreamSupport.stream;

/**
 * Agents index repository.
 */
@Repository
public class AgentsRepository {
    /** */
    protected Ignite ignite;

    /** */
    protected TransactionManager txMgr;

    /** */
    private OneToManyIndex<AgentKey, UUID> backendByAgent;

    /**
     * @param ignite Ignite.
     * @param txMgr Tx manager.
     */
    public AgentsRepository(Ignite ignite, TransactionManager txMgr) {
        this.ignite = ignite;
        this.txMgr = txMgr;

        this.txMgr.registerStarter(() -> {
            backendByAgent = new OneToManyIndex<>(ignite, "wc_backends");

            cleanupBackendIndex();
        });
    }

    /**
     * Get backend nids with agents
     *
     * @param key Agent key
     */
    public Set<UUID> get(AgentKey key) {
        return txMgr.doInTransaction(() -> backendByAgent.get(key));
    }

    /**
     * Add agent to local backend
     *
     * @param accIds Account ids.
     */
    public void add(Set<UUID> accIds) {
        UUID nid = ignite.cluster().localNode().id();

        txMgr.doInTransaction(() -> accIds.forEach(accId -> backendByAgent.add(new AgentKey(accId), nid)));
    }

    /**
     * Add cluster to local backend.
     *
     * @param accIds Account IDs.
     * @param clusterId Cluster ID.
     */
    public void addCluster(Set<UUID> accIds, String clusterId) {
        UUID nid = ignite.cluster().localNode().id();

        txMgr.doInTransaction(() ->
            accIds.forEach(accId -> backendByAgent.add(new AgentKey(accId, clusterId), nid))
        );
    }

    /**
     * Remove agent from local backend
     *
     * @param key Key.
     */
    public void remove(AgentKey key) {
        remove(key, ignite.cluster().localNode().id());
    }

    /**
     * Remove agent from backend
     *
     * @param key Agent key.
     * @param nid Node ID.
     */
    public void remove(AgentKey key, UUID nid) {
        txMgr.doInTransaction(() -> backendByAgent.remove(key, nid));
    }

    /**
     * Has agent for account
     *
     * @param accId Account ID.
     */
    boolean hasAgent(UUID accId) {
        return txMgr.doInTransaction(() -> !backendByAgent.get(new AgentKey(accId)).isEmpty());
    }

    /**
     * Cleanup backend index.
     */
    void cleanupBackendIndex() {
        Collection<UUID> nids = ClusterNodeFunc.nodeIds(ignite.cluster().nodes());

        stream(backendByAgent.cache().spliterator(), false)
            .peek(entry -> entry.getValue().retainAll(nids))
            .filter(entry -> entry.getValue().isEmpty())
            .forEach(entry -> backendByAgent.cache().remove(entry.getKey()));
    }
}
