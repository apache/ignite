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

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;

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
import io.stuart.utils.StdUtil;
import io.stuart.utils.SysUtil;

public class StdCacheServiceImpl extends AbstractCacheService {

    private static volatile CacheService instance;

    private StdCacheServiceImpl() {
        super();
    }

    public static CacheService getInstance() {
        if (instance == null) {
            synchronized (StdCacheServiceImpl.class) {
                if (instance == null) {
                    // initialize instance
                    instance = new StdCacheServiceImpl();
                }
            }
        }

        // return instance
        return instance;
    }

    @Override
    public void start() {
        Logger.log().info("Stuart's standalone cache service is starting...");

        try {
            // start ignite
            ignite = Ignition.start(StdUtil.igniteCfg(new IgniteConfiguration()));
            // active ignite for persistent storage
            ignite.cluster().active(true);
            // set baseline topology
            StdUtil.setBaselineTopology(ignite);

            // get node id
            thisNodeId = ignite.cluster().localNode().id();

            // initialize node cache
            nodeCache = NodeCache.create(ignite, StdUtil.nodeCfg());

            // initialize listener cache
            listenerCache = ListenerCache.create(ignite, StdUtil.listenerCfg());
            // clear listener cache
            listenerCache.clear();

            // initialize connection cache
            connectionCache = ConnectionCache.create(ignite, StdUtil.connectionCfg());
            // clear connection cache
            connectionCache.clear();

            // initialize session cache
            sessionCache = SessionCache.create(ignite, StdUtil.sessionCfg());

            // initialize router cache
            routerCache = RouterCache.create(ignite, StdUtil.routerCfg(), StdUtil.trieCfg());

            // initialize await message cache
            awaitCache = AwaitCache.create(ignite, StdUtil.awaitCfg(), StdUtil.setCfg());

            // initialize queue cache
            queueCache = QueueCache.create(ignite, StdUtil.queueCfg());

            // initialize inflight message cache
            inflightCache = InflightCache.create(ignite, StdUtil.inflightCfg(), StdUtil.setCfg());

            // initialize retain message cache
            retainCache = RetainCache.create(ignite, StdUtil.retainCfg());

            // initialize will message cache
            willCache = WillCache.create(ignite, StdUtil.willCfg());

            // initialize system administrator cache
            adminCache = AdminCache.create(ignite, StdUtil.adminCfg());
            // initialize administrator data
            adminCache.init();

            // check: is local authority mode
            if (ParamConst.AUTH_MODE_LOCAL.equalsIgnoreCase(Config.getAuthMode())) {
                // initialize connect user cache
                userCache = UserCache.create(ignite, StdUtil.userCfg(), adminCache);

                // initialize access control list cache
                aclCache = AclCache.create(ignite, StdUtil.aclCfg());
                // initialize access control list data
                aclCache.init();
            }

            // initialize this node information
            initNode(true);

            Logger.log().info("Stuart's standalone cache service start succeeded.");
        } catch (Exception e) {
            Logger.log().error("Stuart's standalone cache service start failed, exception: {}.", e.getMessage());

            throw new StartException(e);
        }
    }

    @Override
    public void stop() {
        // do nothing...
    }

    @Override
    public <R> R computeCall(UUID nodeId, IgniteCallable<R> job) {
        return null;
    }

    @Override
    public <R, T> R computeApply(UUID nodeId, IgniteClosure<T, R> job, T arg) {
        return null;
    }

    @Override
    public void initNode(boolean isFirstNode) {
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

        // clear node cache
        nodeCache.clear();
        // save new mqtt node
        nodeCache.saveIfAbsent(node);

        // destroy all transient sessions
        destroyTransientSessions(null);
    }

    @Override
    public Set<UUID> remoteNodeIds() {
        return null;
    }

    @Override
    public List<MqttRoute> getClusteredRoutes(String topic, int qos) {
        return null;
    }

}
