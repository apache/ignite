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

package io.stuart.services.cache.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;

import io.stuart.Starter;
import io.stuart.caches.AclCache;
import io.stuart.caches.AdminCache;
import io.stuart.caches.AwaitCache;
import io.stuart.caches.ConnectionCache;
import io.stuart.caches.InflightCache;
import io.stuart.caches.ListenerCache;
import io.stuart.caches.NodeCache;
import io.stuart.caches.QueueCache;
import io.stuart.caches.RetainCache;
import io.stuart.caches.RouterCache;
import io.stuart.caches.SessionCache;
import io.stuart.caches.UserCache;
import io.stuart.caches.WillCache;
import io.stuart.config.Config;
import io.stuart.consts.ParamConst;
import io.stuart.entities.cache.MqttNode;
import io.stuart.entities.internal.MqttRoute;
import io.stuart.enums.Status;
import io.stuart.exceptions.StartException;
import io.stuart.log.Logger;
import io.stuart.services.cache.CacheService;
import io.stuart.utils.ClsUtil;
import io.stuart.utils.SysUtil;

// TODO cluster node use only memory mode(can not be baseline topology node), or use persistence mode
// TODO cluster node can be baseline topology node or not 
public class ClsCacheServiceImpl extends AbstractCacheService {

    private static volatile CacheService instance;

    private IgniteClusterManager clusterManager;

    // declare node left and failed event listener
    private IgnitePredicate<DiscoveryEvent> nodeEventListener = evt -> {
        Logger.log().debug("node : {} - receive another node's left/failed/joined event : {}", thisNodeId, evt.message());

        // get alive server node set
        Set<Object> alives = evt.topologyNodes().stream().filter(n -> !n.isClient()).map(ClusterNode::consistentId).collect(Collectors.toSet());
        // get baseline node set
        Set<Object> baseline = ignite.cluster().currentBaselineTopology().stream().map(BaselineNode::consistentId).collect(Collectors.toSet());
        // get topology version
        final long topologyVersion = evt.topologyVersion();

        if (!alives.equals(baseline)) {
            ((IgniteEx) ignite).context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(Config.getClusterBltRebalanceTimeMs()) {
                @Override
                public void onTimeout() {
                    if (ignite.cluster().topologyVersion() == topologyVersion) {
                        ignite.cluster().setBaselineTopology(topologyVersion);
                    }
                }
            });
        }

        if (evt.type() == EventType.EVT_NODE_LEFT || evt.type() == EventType.EVT_NODE_FAILED) {
            // get node id
            UUID eventNodeId = evt.eventNode().id();
            // get lock
            Lock lock = nodeCache.lock(eventNodeId);

            if (lock != null && lock.tryLock()) {
                try {
                    // get mqtt node
                    MqttNode mqttNode = nodeCache.get(eventNodeId);

                    // check: mqtt node is not null and status is running
                    if (mqttNode != null && Status.Running == Status.valueOf(mqttNode.getStatus())) {
                        // update node status
                        nodeCache.update(eventNodeId, Status.Stopped);

                        // destroy all transient sessions of the left/failed node
                        destroyTransientSessions(eventNodeId);
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        // continue listen next event
        return true;
    };

    private ClsCacheServiceImpl() {
        super();
    }

    ClsCacheServiceImpl(IgniteClusterManager clusterManager) {
        super();
        this.clusterManager = clusterManager;
    }

    public static CacheService getInstance() {
        if (instance == null) {
            synchronized (ClsCacheServiceImpl.class) {
                if (instance == null) {
                    // initialize instance
                    instance = new ClsCacheServiceImpl();
                }
            }
        }

        // return instance
        return instance;
    }

    // get vert.x cluster manager
    public IgniteClusterManager getClusterManager(){
        if(clusterManager==null) {
            clusterManager = new IgniteClusterManager();
        }
        return clusterManager;
    }

    @Override
    public void start() {
        Logger.log().info("Stuart's clustered cache service is starting...");

        try {
            // TODO blinking baseline topology node sometimes unable to connect to cluster.
            // exception: Restoring of BaselineTopology history has failed, expected history
            // item not found for id=0
            // also see: IGNITE-8879. This bug will be fixed in version 2.8
            // start ignite
            ClsUtil.igniteCfg(getClusterManager().getIgniteConfiguration());
            ignite = getClusterManager().getIgniteInstance();
            // active ignite for persistent storage
            ignite.cluster().active(true);
            // set baseline topology
            Collection<ClusterNode> clusterNodes = ClsUtil.setBaselineTopology(ignite);

            // get node id
            thisNodeId = ignite.cluster().localNode().id();

            // initialize node cache
            nodeCache = NodeCache.create(ignite, ClsUtil.nodeCfg());

            // initialize listener cache
            listenerCache = ListenerCache.create(ignite, ClsUtil.listenerCfg());
            // clear listener cache
            listenerCache.clear();

            // initialize connection cache
            connectionCache = ConnectionCache.create(ignite, ClsUtil.connectionCfg());
            // clear connection cache
            connectionCache.clear();

            // initialize session cache
            sessionCache = SessionCache.create(ignite, ClsUtil.sessionCfg());

            // initialize router cache
            routerCache = RouterCache.create(ignite, ClsUtil.routerCfg(), ClsUtil.trieCfg());

            // initialize await message cache
            awaitCache = AwaitCache.create(ignite, ClsUtil.awaitCfg(), ClsUtil.setCfg());

            // initialize queue cache
            queueCache = QueueCache.create(ignite, ClsUtil.queueCfg());

            // initialize inflight message cache
            inflightCache = InflightCache.create(ignite, ClsUtil.inflightCfg(), ClsUtil.setCfg());

            // initialize retain message cache
            retainCache = RetainCache.create(ignite, ClsUtil.retainCfg());

            // initialize will message cache
            willCache = WillCache.create(ignite, ClsUtil.willCfg());

            // initialize system administrator cache
            adminCache = AdminCache.create(ignite, ClsUtil.adminCfg());
            // initialize administrator data
            adminCache.init();

            // check: is local authority mode
            if (ParamConst.AUTH_MODE_LOCAL.equalsIgnoreCase(Config.getAuthMode())) {
                // initialize connect user cache
                userCache = UserCache.create(ignite, ClsUtil.userCfg(), adminCache);

                // initialize access control list cache
                aclCache = AclCache.create(ignite, ClsUtil.aclCfg());
                // initialize access control list data
                aclCache.init();
            }

            // set node event listener
            ignite.events().localListen(nodeEventListener, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_JOINED);

            // initialize this node information
            initNode(ClsUtil.isFirstNode(clusterNodes));

            Logger.log().info("Stuart's clustered cache service start succeeded.");
        } catch (Exception e) {
            Logger.log().error("Stuart's clustered cache service start failed, exception: {}.", e.getMessage());

            throw new StartException(e);
        }
    }

    @Override
    public void stop() {
        // do nothing...
    }

    @Override
    public <R> R computeCall(UUID nodeId, IgniteCallable<R> job) {
        if (nodeId == null || job == null) {
            return null;
        }

        // get cluster group
        ClusterGroup group = ignite.cluster().forNodeId(nodeId);

        // check: cluster group is not empty
        if (group != null && !ClsUtil.isEmptyGroup(group)) {
            // get ignite compute
            IgniteCompute compute = ignite.compute(group);

            try {
                // return compute result
                return compute.call(job);
            } catch (IgniteException e) {
                Logger.log().debug("call ignite job on node({}) has an exception: {}", nodeId, e.getMessage());
            }
        }

        return null;
    }

    @Override
    public <R, T> R computeApply(UUID nodeId, IgniteClosure<T, R> job, T arg) {
        if (nodeId == null || job == null) {
            return null;
        }

        // get cluster group
        ClusterGroup group = ignite.cluster().forNodeId(nodeId);

        // check: cluster group is not empty
        if (group != null && !ClsUtil.isEmptyGroup(group)) {
            // get ignite compute
            IgniteCompute compute = ignite.compute(group);

            try {
                // return compute result
                return compute.apply(job, arg);
            } catch (IgniteException e) {
                Logger.log().debug("apply ignite closure on node({}) has an exception: {}", nodeId, e.getMessage());
            }
        }

        return null;
    }

    @Override
    public void initNode(boolean isFirstNode) {
        // get old mqtt node
        MqttNode old = nodeCache.get(Config.getInstanceId(), Config.getInstanceListenAddr());

        // initialize new mqtt node
        MqttNode node = new MqttNode();
        // set new mqtt node attributes
        node.setNodeId(thisNodeId);
        node.setInstanceId(Config.getInstanceId());
        node.setListenAddr(Config.getInstanceListenAddr());
        node.setVersion(Starter.class.getPackage().getImplementationVersion());
        node.setLocalAuth(ParamConst.AUTH_MODE_LOCAL.equalsIgnoreCase(Config.getAuthMode()));
        node.setJavaVersion(SysUtil.getJavaAndJvmInfo());
        node.setStatus(Status.Running.value());

        Logger.log().debug("node : {} - cluster node startup information is {}, is the first node of cluster? {}", thisNodeId, node, isFirstNode);
        Logger.log().debug("node : {} - cluster node startup, the old node is {}", thisNodeId, old);

        if (isFirstNode) {
            // clear node cache
            nodeCache.clear();

            // destroy all transient sessions
            destroyTransientSessions(null);
        } else if (old != null) {
            // delete old mqtt node
            nodeCache.delete(old.getNodeId());

            Logger.log().debug("node : {} - cluster node startup, delete the old node {}", thisNodeId, old.getNodeId());
        }

        // save mqtt node
        nodeCache.saveIfAbsent(node);
    }

    @Override
    public Set<UUID> remoteNodeIds() {
        return ignite.cluster().forRemotes().nodes().stream().map(n -> n.id()).collect(Collectors.toSet());
    }

    @Override
    public List<MqttRoute> getClusteredRoutes(String topic, int qos) {
        return routerCache.getClusteredRoutes(topic, qos);
    }

}
