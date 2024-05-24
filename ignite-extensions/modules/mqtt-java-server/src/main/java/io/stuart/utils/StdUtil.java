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

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

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

public class StdUtil {

    // if use local cache mode, then if ignite cache use withExpiryPolicy function,
    // system will throw 'Topology is not initialized' exception. Why?
    // private static final CacheMode local = CacheMode.LOCAL;

    private static final CacheMode partitioned = CacheMode.PARTITIONED;

    private static final CacheMode replicated = CacheMode.REPLICATED;

    private static final CacheAtomicityMode atomic = CacheAtomicityMode.ATOMIC;

    private static final CacheAtomicityMode trans = CacheAtomicityMode.TRANSACTIONAL;

    private static final CacheAtomicityMode snapshot = CacheAtomicityMode.TRANSACTIONAL;

    public static IgniteConfiguration igniteCfg(IgniteConfiguration cfg) {
        // get ignite configuration
        CacheUtil.igniteCfg(cfg, true, false, EventType.EVT_CACHE_OBJECT_EXPIRED);

        // set discovery spi
        cfg.setDiscoverySpi(localDiscoverySpi());

        // return ignite configuration
        return cfg;
    }

    public static CacheConfiguration<UUID, MqttNode> nodeCfg() {
        return memTransCfg(CacheConst.NODE_NAME, UUID.class, MqttNode.class);
    }

    public static CacheConfiguration<String, MqttListener> listenerCfg() {
        return memAtomicCfg(CacheConst.LISTENER_NAME, String.class, MqttListener.class);
    }

    public static CacheConfiguration<String, MqttConnection> connectionCfg() {
        return memTransCfg(CacheConst.CONNECTION_NAME, String.class, MqttConnection.class);
    }

    public static CacheConfiguration<String, MqttSession> sessionCfg() {
        return transCfg(CacheConst.SESSION_NAME, String.class, MqttSession.class);
    }

    public static CacheConfiguration<MqttRouterKey, MqttRouter> routerCfg() {
        return snapshotCfg(CacheConst.ROUTER_NAME, MqttRouterKey.class, MqttRouter.class);
    }

    public static CacheConfiguration<MqttTrieKey, MqttTrie> trieCfg() {
        return snapshotCfg(CacheConst.TRIE_NAME);
    }

    public static CacheConfiguration<MqttAwaitMessageKey, MqttAwaitMessage> awaitCfg() {
        return atomicCfg(CacheConst.AWAIT_MESSAGE_NAME);
    }

    public static CacheConfiguration<MqttMessageKey, MqttMessage> inflightCfg() {
        return atomicCfg(CacheConst.INFLIGHT_MESSAGE_NAME);
    }

    public static CacheConfiguration<String, MqttRetainMessage> retainCfg() {
        return atomicCfg(CacheConst.RETAIN_NAME);
    }

    public static CacheConfiguration<String, MqttWillMessage> willCfg() {
        return atomicCfg(CacheConst.WILL_NAME);
    }

    public static CacheConfiguration<String, MqttUser> userCfg() {
        return atomicCfg(CacheConst.USER_NAME, String.class, MqttUser.class);
    }

    public static CacheConfiguration<Long, MqttAcl> aclCfg() {
        return transCfg(CacheConst.ACL_NAME, Long.class, MqttAcl.class);
    }

    public static CacheConfiguration<String, MqttAdmin> adminCfg() {
        return atomicCfg(CacheConst.ADMIN_NAME, String.class, MqttAdmin.class);
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
        Collection<ClusterNode> nodes = ignite.cluster().forLocal().nodes();

        // set baseline topology
        ignite.cluster().setBaselineTopology(nodes);

        // return node collection
        return nodes;
    }

    private static TcpDiscoverySpi localDiscoverySpi() {
        // get local address for ip finder, ip:port
        String localAddress = ParamConst.STD_LOCAL_IP + SysConst.COLON + ParamConst.STD_LOCAL_PORT;

        // initialize ip finder
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        // set addresses
        ipFinder.setAddresses(Arrays.asList(localAddress));

        // initialize tcp discovery spi
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        // set local ip
        spi.setLocalAddress(ParamConst.STD_LOCAL_IP);
        // set local port
        spi.setLocalPort(ParamConst.STD_LOCAL_PORT);
        // set local port range
        spi.setLocalPortRange(0);
        // set ip finder
        spi.setIpFinder(ipFinder);

        // return tcp discovery spi
        return spi;
    }

    private static <K, V> CacheConfiguration<K, V> memAtomicCfg(String name, Class<?>... indexes) {
        return CacheUtil.<K, V>memCacheCfg(name, replicated, atomic, CacheConst.DEF_CACHE_BACKUPS, indexes);
    }

    private static <K, V> CacheConfiguration<K, V> memTransCfg(String name, Class<?>... indexes) {
        return CacheUtil.<K, V>memCacheCfg(name, replicated, trans, CacheConst.DEF_CACHE_BACKUPS, indexes);
    }

    private static <K, V> CacheConfiguration<K, V> atomicCfg(String name, Class<?>... indexes) {
        return CacheUtil.<K, V>cacheCfg(name, replicated, atomic, CacheConst.DEF_CACHE_BACKUPS, indexes);
    }

    private static <K, V> CacheConfiguration<K, V> transCfg(String name, Class<?>... indexes) {
        return CacheUtil.<K, V>cacheCfg(name, replicated, trans, CacheConst.DEF_CACHE_BACKUPS, indexes);
    }

    private static <K, V> CacheConfiguration<K, V> snapshotCfg(String name, Class<?>... indexes) {
        return CacheUtil.<K, V>cacheCfg(name, replicated, snapshot, CacheConst.DEF_CACHE_BACKUPS, indexes);
    }

}
