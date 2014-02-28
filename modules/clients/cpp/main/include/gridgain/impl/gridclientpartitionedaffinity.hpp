// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTPARTITIONEDAFFINITY__HPP_INCLUDED
#define GRIDCLIENTPARTITIONEDAFFINITY__HPP_INCLUDED

#include <vector>
#include <string>
#include <memory>

#include "gridgain/gridclientdataaffinity.hpp"
#include "gridgain/impl/gridconsistenhashimpl.hpp"
#include "gridgain/impl/gridclienttopology.hpp"

class GridClientNode;

/** Function that defines a value to be used for hash ID. */
typedef std::function<GridClientVariant (const GridClientNode&)> GridHashIdResolver;

/**
 * Affinity function for partitioned cache. This function supports the following
 * configuration:
 * <ul>
 * <li>
 *      <tt>backups</tt> - Use ths flag to control how many back up nodes will be
 *      assigned to every key. The default value is defined by {@link #DFLT_BACKUP_CNT}.
 * </li>
 * <li>
 *      <tt>replicas</tt> - Generally the more replicas a node gets, the more key assignments
 *      it will receive. You can configure different number of replicas for a node by
 *      setting user attribute with name {@link #getReplicaCountAttributeName()} to some
 *      number. Default value is <tt>512</tt> defined by {@link #DFLT_REPLICA_CNT} constant.
 * </li>
 * <li>
 *      <tt>backupFilter</tt> - Optional filter for back up nodes. If provided, then only
 *      nodes that pass this filter will be selected as backup nodes and only nodes that
 *      don't pass this filter will be selected as primary nodes. If not provided, then
 *      primary and backup nodes will be selected out of all nodes available for this cache.
 *      <p>
 *      NOTE: In situations where there are no primary nodes at all, i.e. no nodes for which backup
 *      filter returns <tt>false</tt>, first backup node for the key will be considered primary.
 * </li>
 * </ul>
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientPartitionAffinity: public GridClientDataAffinity {
public:
    /** Default number of partitions. */
    static const int DFLT_PARTITION_CNT = 10000;

    /** Default number of backups. */
    static const int DFLT_BACKUP_CNT = 1;

    /** Default replica count for partitioned caches. */
    static const int DFLT_REPLICA_CNT = 128;

    /**
     * Name of node attribute to specify number of replicas for a node.
     * Default value is <tt>gg:affinity:node:replicas</tt>.
     */
    static const char* DFLT_REPLICA_CNT_ATTR_NAME;

    /**
     * Default constructor.
     */
    GridClientPartitionAffinity();

    /** Vitrual destructor. */
    virtual ~GridClientPartitionAffinity();

    /**
     * Maps the key to the primary node.
     *
     * @param nodes The set of nodes representing cache topology.
     * @param key The key to be mapped to a node.
     *
     * @return The affinity node for the given key.
     */
    virtual TGridClientNodePtr getNode(const TNodesSet& nodes, const GridHasheableObject& key);

    /**
     * Gets default count of virtual replicas on the consistent hash ring.
     * <p>
     * To determine node replicas, node attribute with {@link #getReplicaCountAttributeName()}
     * name will be checked first. If it is absent, then this value will be used.
     *
     * @return Count of virtual replicas in consistent hash ring.
     */
    int getDefaultReplicas() const;

    /**
     * Sets default count of virtual replicas in consistent hash ring.
     * <p>
     * To determine node replicas, node attribute with {@link #getReplicaCountAttributeName} name
     * will be checked first. If it is absent, then this value will be used.
     *
     * @param replicas Count of virtual replicas in consistent hash ring.s
     */
    void setDefaultReplicas(int replicas);

    /**
     * Gets count of key backups for redundancy.
     *
     * @return Key backup count.
     */
    int getKeyBackups() const;

    /**
     * Sets count of key backups for redundancy.
     *
     * @param backups Key backup count.
     */
    void setKeyBackups(int backups);

    /**
     * Gets total number of key partitions. To ensure that all partitions are
     * equally distributed across all nodes, please make sure that this
     * number is significantly larger than a number of nodes. Also, partition
     * size should be relatively small. Try to avoid having partitions with more
     * than quarter million keys.
     * <p>
     * Note that for fully replicated caches this method should always
     * return <tt>1</tt>.
     *
     * @return Total partition count.
     */
    int getPartitions() const;

    /**
     * Sets total number of partitions.
     *
     * @param parts Total number of partitions.
     */
    void setPartitions(int parts);

    /**
     * Gets optional backup filter. If not <tt>null</tt>, then primary nodes will be
     * selected from all nodes outside of this filter, and backups will be selected
     * from all nodes inside it.
     *
     * @return Optional backup filter.
     */
    const TGridClientNodePredicatePtr getBackupFilter();

    /**
     * Sets optional backup filter. If provided, then primary nodes will be selected
     * from all nodes outside of this filter, and backups will be selected from all
     * nodes inside it.
     *
     * @param backupFilter Optional backup filter.
     */
    void setBackupFilter(const TGridClientNodePredicatePtr backupFilter);

    /**
     * Gets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_CNT_ATTR_NAME}.
     *
     * @return User attribute name for replica count for a node.
     */
    std::string getReplicaCountAttributeName() const;

    /**
     * Sets optional attribute name for replica count. If not provided, the
     * default is {@link #DFLT_REPLICA_CNT_ATTR_NAME}.
     *
     * @param attrName User attribute name for replica count for a node.
     */
    void setReplicaCountAttributeName(const std::string& attrName);

    /**
     * Gets function for defining ID for node hashing.
     *
     * @return Hash ID resolver function.
     */
    GridHashIdResolver getHashIdResolver() const;

    /**
     * Sets function for defining ID for node hashing.
     *
     * @param hashIdResolver Hash ID resolver function.
     */
    void setHashIdResolver(GridHashIdResolver& hashIdResolver);

    /**
     * Cleans up removed nodes.
     * @param node Node that was removed from topology.
     */
    void checkRemoved(const GridClientNode& node);

    /**
     * @param n Node.
     * @return Replicas.
     */
    int getReplicas(const GridClientNode& n);

private:
    /**
     * Add the id to the consistent hash.
     *
     * @param id Node ID to add.
     * @param replicas Replicas.
     */
    void add(const GridClientNode& node, int replicas);

    /**
     * Looks up a node with the given UUID. The id is expected to exist in the set of nodes.
     *
     * @param id The id of the node to look up.
     * @param nodes The set of node to search.
     *
     * @return The client node with the matching uuid.
     */
    TGridClientNodePtr findNode(const GridUuid& id, const TNodesSet& nodes) const;

    /** Hasher for this affinity function. */
    GridClientConsistentHashImpl nodeHash;

    /** Number of partitions. */
    int parts;

    /** Number of replicas. */
    int replicas;

    /** Number of backups. */
    int backups;

    /** Attribute name for replicas count. */
    std::string attrName;

    /** Nodes IDs. */
    std::set<GridUuid> addedNodes;

    /** Optional backup filter. */
    TGridClientNodePredicatePtr backupFilter;

    /** Primary filter. */
    TGridClientNodePredicatePtr primaryFilter;

    /** Hash ID resolver function. */
    GridHashIdResolver hashIdResolver;

    /** Mutex. */
    boost::mutex mux;
};

#endif // GRIDCLIENTPARTITIONEDAFFINITY__INCLUDED
