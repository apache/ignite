/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.consts.ParamConst;
import io.stuart.consts.SysConst;
import io.stuart.entities.auth.MqttAcl;
import io.stuart.entities.auth.MqttAdmin;
import io.stuart.entities.auth.MqttUser;
import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttAwaitMessageKey;
import io.stuart.entities.cache.MqttConnection;
import io.stuart.entities.cache.MqttListener;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.cache.MqttMessageKey;
import io.stuart.entities.cache.MqttNode;
import io.stuart.entities.cache.MqttRetainMessage;
import io.stuart.entities.cache.MqttRouter;
import io.stuart.entities.cache.MqttRouterKey;
import io.stuart.entities.cache.MqttSession;
import io.stuart.entities.cache.MqttTrie;
import io.stuart.entities.cache.MqttTrieKey;
import io.stuart.entities.cache.MqttWillMessage;

public class ClsUtil {

    private static final CacheMode partitioned = CacheMode.PARTITIONED;

    private static final CacheMode replicated = CacheMode.REPLICATED;

    private static final CacheAtomicityMode atomic = CacheAtomicityMode.ATOMIC;

    private static final CacheAtomicityMode trans = CacheAtomicityMode.TRANSACTIONAL;

    private static final CacheAtomicityMode snapshot = CacheAtomicityMode.TRANSACTIONAL;

    public static IgniteConfiguration igniteCfg(IgniteConfiguration cfg) {
        // get cluster mode
        String clusterMode = Config.getClusterMode();
        // get ignite configuration
        CacheUtil.igniteCfg(cfg,true, false, evnets());

        if (ParamConst.CLUSTER_MODE_VMIP.equalsIgnoreCase(clusterMode)) {
            cfg.setDiscoverySpi(vmipDiscoverySpi());
        } else if (ParamConst.CLUSTER_MODE_ZK.equalsIgnoreCase(clusterMode)) {
            cfg.setDiscoverySpi(zookeeperDiscoverySpi());
        }
        // return ignite configuration
        return cfg;
    }

    public static CacheConfiguration<UUID, MqttNode> nodeCfg() {
        return memCacheCfg(CacheConst.NODE_NAME, replicated, trans, CacheConst.DEF_CACHE_BACKUPS, UUID.class, MqttNode.class);
    }

    public static CacheConfiguration<String, MqttListener> listenerCfg() {
        return memCacheCfg(CacheConst.LISTENER_NAME, partitioned, atomic, CacheConst.DEF_CACHE_BACKUPS, String.class, MqttListener.class);
    }

    public static CacheConfiguration<String, MqttConnection> connectionCfg() {
        return memCacheCfg(CacheConst.CONNECTION_NAME, partitioned, trans, CacheConst.DEF_CACHE_BACKUPS, String.class, MqttConnection.class);
    }

    public static CacheConfiguration<String, MqttSession> sessionCfg() {
        return cacheCfg(CacheConst.SESSION_NAME, partitioned, trans, String.class, MqttSession.class);
    }

    public static CacheConfiguration<MqttRouterKey, MqttRouter> routerCfg() {
        return cacheCfg(CacheConst.ROUTER_NAME, replicated, snapshot, CacheConst.DEF_CACHE_BACKUPS, MqttRouterKey.class, MqttRouter.class);
    }

    public static CacheConfiguration<MqttTrieKey, MqttTrie> trieCfg() {
        return cacheCfg(CacheConst.TRIE_NAME, replicated, snapshot, CacheConst.DEF_CACHE_BACKUPS);
    }

    public static CacheConfiguration<MqttAwaitMessageKey, MqttAwaitMessage> awaitCfg() {
        return cacheCfg(CacheConst.AWAIT_MESSAGE_NAME, partitioned, atomic);
    }

    public static CacheConfiguration<MqttMessageKey, MqttMessage> inflightCfg() {
        return cacheCfg(CacheConst.INFLIGHT_MESSAGE_NAME, partitioned, atomic);
    }

    public static CacheConfiguration<String, MqttRetainMessage> retainCfg() {
        return cacheCfg(CacheConst.RETAIN_NAME, partitioned, atomic);
    }

    public static CacheConfiguration<String, MqttWillMessage> willCfg() {
        return cacheCfg(CacheConst.WILL_NAME, partitioned, atomic);
    }

    public static CacheConfiguration<String, MqttUser> userCfg() {
        return cacheCfg(CacheConst.USER_NAME, replicated, atomic, CacheConst.DEF_CACHE_BACKUPS, String.class, MqttUser.class);
    }

    public static CacheConfiguration<Long, MqttAcl> aclCfg() {
        return cacheCfg(CacheConst.ACL_NAME, replicated, trans, CacheConst.DEF_CACHE_BACKUPS, Long.class, MqttAcl.class);
    }

    public static CacheConfiguration<String, MqttAdmin> adminCfg() {
        return cacheCfg(CacheConst.ADMIN_NAME, replicated, atomic, CacheConst.DEF_CACHE_BACKUPS, String.class, MqttAdmin.class);
    }

    public static CollectionConfiguration queueCfg() {
        return CacheUtil.collectionCfg(true, atomic, partitioned, CacheConst.DEF_QUEUE_BACKUPS, CacheConst.DEF_QUEUE_OFFHEAP_MAX_MEMORY);
    }

    public static CollectionConfiguration setCfg() {
        return CacheUtil.collectionCfg(true, atomic, partitioned, CacheConst.DEF_SET_BACKUPS, CacheConst.DEF_SET_OFFHEAP_MAX_MEMORY);
    }

    public static Collection<ClusterNode> setBaselineTopology(Ignite ignite) {
        if (ignite == null) {
            return null;
        }

        // get cluster node collection
        Collection<ClusterNode> nodes = ignite.cluster().forServers().nodes();

        // set baseline topology
        ignite.cluster().setBaselineTopology(nodes);

        // return node collection
        return nodes;
    }

    public static boolean isFirstNode(Collection<ClusterNode> nodes) {
        if (nodes != null && nodes.size() <= 1) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isEmptyGroup(ClusterGroup group) {
        if (group == null) {
            return true;
        }

        Collection<ClusterNode> nodes = group.nodes();

        if (nodes == null || nodes.isEmpty()) {
            return true;
        }

        return false;
    }

    private static int[] evnets() {
        List<Integer> events = new ArrayList<>();

        events.add(EventType.EVT_NODE_JOINED);
        events.add(EventType.EVT_NODE_LEFT);
        events.add(EventType.EVT_NODE_FAILED);
        events.add(EventType.EVT_CACHE_OBJECT_EXPIRED);

        return events.stream().mapToInt(Integer::valueOf).toArray();
    }

    private static TcpDiscoverySpi vmipDiscoverySpi() {
        // cluster addresses
        String clusterAddresses = null;
        // get local address for ip finder, ip:port
        String localAddress = ParamConst.STD_LOCAL_IP + SysConst.COLON + ParamConst.STD_LOCAL_PORT;

        if (StringUtils.isBlank(Config.getVmipAddresses())) {
            clusterAddresses = localAddress;
        } else {
            clusterAddresses = Config.getVmipAddresses();
        }

        // initialize ip finder
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        // set addresses
        ipFinder.setAddresses(Arrays.asList(clusterAddresses));

        // initialize tcp discovery spi
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        // set ip finder
        spi.setIpFinder(ipFinder);

        // return tcp discovery spi
        return spi;
    }

    private static ZookeeperDiscoverySpi zookeeperDiscoverySpi() {
        // initialize zookeeper discovery spi
        ZookeeperDiscoverySpi spi = new ZookeeperDiscoverySpi();

        // set zookeeper discovery spi attributes
        spi.setZkConnectionString(Config.getZkConnectString());
        spi.setZkRootPath(Config.getZkRootPath());
        spi.setJoinTimeout(Config.getZkJoinTimeoutMs());
        spi.setSessionTimeout(Config.getZkSessionTimeoutMs());
        spi.setClientReconnectDisabled(!Config.isZkReconnectEnable());

        // return zookeeper discovery spi
        return spi;
    }

    private static <K, V> CacheConfiguration<K, V> memCacheCfg(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode, int backups,
            Class<?>... indexes) {

        // return memory cache configuration
        return CacheUtil.<K, V>memCacheCfg(name, cacheMode, atomicityMode, backups, indexes);
    }

    private static <K, V> CacheConfiguration<K, V> cacheCfg(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode, Class<?>... indexes) {

        // return cache configuration
        return cacheCfg(name, cacheMode, atomicityMode, Config.getClusterStorageBackups(), indexes);
    }

    private static <K, V> CacheConfiguration<K, V> cacheCfg(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode, int backups,
            Class<?>... indexes) {

        // return cache configuration
        return CacheUtil.<K, V>cacheCfg(name, cacheMode, atomicityMode, backups, indexes);
    }

}
