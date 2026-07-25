

package org.apache.ignite.console.web.socket;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.util.lang.ClusterNodeFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Repository;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;


/**
 * Clusters index repository.
 */
@Repository
public class ClustersRepository {
    /** Default for cluster topology expire. */
    private static final long DEFAULT_CLUSTER_CLEANUP = MINUTES.toMillis(10);

    /** */
    private static final Logger log = LoggerFactory.getLogger(ClustersRepository.class);

    /** */
    protected Ignite ignite;

    /** */
    protected TransactionManager txMgr;

    /** */
    private CacheHolder<String, TopologySnapshot> clusters;

    /** */
    private OneToManyIndex<UserKey, ClusterSession> clusterIdsByUser;

    /**
     * @param ignite Ignite.
     * @param txMgr Tx manager.
     */
    public ClustersRepository(Ignite ignite, TransactionManager txMgr) {
        this.ignite = ignite;
        this.txMgr = txMgr;


        this.txMgr.registerStarter(() -> {
            clusters = new CacheHolder<>(ignite, "wc_clusters");
            clusterIdsByUser = new OneToManyIndex<>(ignite, "wc_clusters_idx");

            cleanupClusterIndex();
        });
    }

    /**
     * Get latest topology for user
     *
     * @param user User.
     */
    public Set<TopologySnapshot> get(UserKey user) {
        return txMgr.doInTransaction(() ->
            Optional.ofNullable(clusterIdsByUser.get(user))
                .orElseGet(Collections::emptySet).stream()
                .map(ClusterSession::getClusterId)
                .distinct()
                .map(clusters::get)
                .collect(toSet())
        );
    }

    /**
     * @param users Users.
     * @return Collection of cluster IDs.
     */
    public Set<String> clusters(Set<UserKey> users) {
        return txMgr.doInTransaction(() ->
            clusterIdsByUser
                .getAll(users)
                .stream()
                .map(ClusterSession::getClusterId)
                .collect(toSet()));
    }

    /**
     * @param users Users.
     * @return Collection of topologies.
     */
    public Collection<TopologySnapshot> topologies(Set<UserKey> users) {
        return txMgr.doInTransaction(() -> clusters.cache().getAll(clusters(users)).values());
    }

    /**
     * Get latest topology for clusters
     *
     * @param clusterId Cluster ID.
     */
    public TopologySnapshot get(String clusterId) {
        return txMgr.doInTransaction(() -> clusters.get(clusterId));
    }

    /**
     * Get latest topology for clusters
     *
     * @param clusterIds Cluster ids.
     */
    public Set<TopologySnapshot> get(Set<String> clusterIds) {
        return txMgr.doInTransaction(() -> clusterIds.stream().map(clusters::get).collect(toSet()));
    }

    /**
     * Find cluster with same topology and get it's cluster ID.
     * 
     * @param top Topology.
     */
    public String findClusterId(TopologySnapshot top) {
        return txMgr.doInTransaction(() ->
            stream(clusters.cache().spliterator(), false)
                .filter(e -> e.getValue().sameNodes(top))
                .map(Cache.Entry::getKey)
                .findFirst()
                .orElse(null)
        );
    }

    /**
     * Save topology in cache.
     *
     * @param accIds Account ids.
     * @param top Topology.
     */
    public TopologySnapshot getAndPut(Set<UUID> accIds, TopologySnapshot top) {
        UUID nid = ignite.cluster().localNode().id();

        return txMgr.doInTransaction(() -> {
            ClusterSession clusterSes = new ClusterSession(nid, top.getId());

            for (UUID accId : accIds)
                clusterIdsByUser.add(new UserKey(accId, top.isDemo()), clusterSes);

            return clusters.getAndPut(top.getId(), top);
        });
    }

    /**
     * Remove cluster from local backend.
     *
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     */
    public void remove(UUID accId, String clusterId) {
        UUID nid = ignite.cluster().localNode().id();

        ClusterSession clusterSes = new ClusterSession(nid, clusterId);

        txMgr.doInTransaction(() -> {
            boolean demo = clusters.get(clusterId).isDemo();

            clusterIdsByUser.remove(new UserKey(accId, demo), clusterSes);
        });
    }

    /**
     * Has demo for account
     *
     * @param accId Account ID.
     */
    public boolean hasDemo(UUID accId) {
        return !F.isEmpty(clusterIdsByUser.get(new UserKey(accId, true)));
    }

    /**
     * Cleanup cluster index.
     */
    void cleanupClusterIndex() {
        Collection<UUID> nids = ClusterNodeFunc.nodeIds(ignite.cluster().nodes());

        stream(clusterIdsByUser.cache().spliterator(), false)
            .peek(entry -> {
                Set<ClusterSession> activeClusters =
                    entry.getValue().stream().filter(cluster -> nids.contains(cluster.getNid())).collect(toSet());

                entry.getValue().removeAll(activeClusters);
            })
            .filter(entry -> !entry.getValue().isEmpty())
            .forEach(entry -> clusterIdsByUser.removeAll(entry.getKey(), entry.getValue()));
    }

    /**
     * Periodically cleanup expired cluster topologies.
     */
    @Scheduled(initialDelay = 0, fixedRate = 60_000)
    public void cleanupClusterHistory() {
        txMgr.doInTransaction(() -> {
            Set<String> clusterIds = stream(clusters.cache().spliterator(), false)
                .filter(entry -> entry.getValue().isExpired(DEFAULT_CLUSTER_CLEANUP))
                .map(Cache.Entry::getKey)
                .collect(toSet());

            if (!F.isEmpty(clusterIds)) {
                clusters.cache().removeAll(clusterIds);

                log.debug("Cleared topology for clusters: {}", clusterIds);
            }
        });
    }
}
