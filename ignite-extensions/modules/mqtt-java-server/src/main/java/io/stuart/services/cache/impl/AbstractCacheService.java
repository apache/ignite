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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;

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
import io.stuart.closures.DestroySessionClosure;
import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.consts.MetricsConst;
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
import io.stuart.entities.cache.MqttSession;
import io.stuart.entities.cache.MqttWillMessage;
import io.stuart.entities.internal.MqttRoute;
import io.stuart.enums.Status;
import io.stuart.ext.collections.BoundedIgniteMap;
import io.stuart.ext.collections.BoundedIgniteMapUnsafe;
import io.stuart.ext.collections.BoundedIgniteQueue;
import io.stuart.services.cache.CacheService;
import io.stuart.services.metrics.MetricsService;
import io.stuart.utils.AesUtil;
import io.stuart.utils.MetricsUtil;
import io.stuart.utils.SysUtil;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.messages.MqttPublishMessage;

public abstract class AbstractCacheService implements CacheService {

    protected Ignite ignite;

    protected UUID thisNodeId;

    protected NodeCache nodeCache;

    protected ListenerCache listenerCache;

    protected ConnectionCache connectionCache;

    protected SessionCache sessionCache;

    protected RouterCache routerCache;

    protected AwaitCache awaitCache;

    protected InflightCache inflightCache;

    protected QueueCache queueCache;

    protected RetainCache retainCache;

    protected WillCache willCache;

    protected UserCache userCache;

    protected AclCache aclCache;

    protected AdminCache adminCache;

    protected DestroySessionClosure destroySessionClosure;

    public AbstractCacheService() {
        // initialize destroy session closure
        this.destroySessionClosure = new DestroySessionClosure(this);
    }

    @Override
    public abstract void start();

    @Override
    public abstract void stop();

    @Override
    public Ignite getIgnite() {
        return ignite;
    }

    @Override
    public void eventLocalListen(IgnitePredicate<? extends Event> listener, int... types) {
        ignite.events().localListen(listener, types);
    }

    @Override
    public abstract <R> R computeCall(UUID nodeId, IgniteCallable<R> job);

    @Override
    public abstract <R, T> R computeApply(UUID nodeId, IgniteClosure<T, R> job, T arg);

    @Override
    public abstract void initNode(boolean firstNode);

    @Override
    public void updateNodeSysRuntimeInfo() {
        MqttNode node = new MqttNode();

        node.setNodeId(thisNodeId);
        node.setThread(SysUtil.getThreadInfo());
        node.setCpu(SysUtil.getLoadAverageInfo());
        node.setHeap(SysUtil.getHeapMemoryInfo());
        node.setOffHeap(SysUtil.getOffHeapMemoryInfo());
        node.setMaxFileDescriptors(SysUtil.getMaxFileDescriptors());

        nodeCache.update(node);
    }

    @Override
    public UUID localNodeId() {
        return thisNodeId;
    }

    @Override
    public abstract Set<UUID> remoteNodeIds();

    @Override
    public boolean isLocalAuth(UUID nodeId) {
        if (nodeId == null) {
            return false;
        }

        MqttNode node = nodeCache.get(nodeId);

        if (node == null) {
            return false;
        } else {
            return node.isLocalAuth();
        }
    }

    @Override
    public MqttNode getNode(UUID nodeId) {
        return nodeCache.get(nodeId);
    }

    @Override
    public List<MqttNode> getNodes(Status status) {
        return nodeCache.query(status);
    }

    @Override
    public void saveListener(MqttListener listener) {
        listenerCache.saveIfAbsent(listener);
    }

    @Override
    public List<MqttListener> getListeners() {
        // get listener
        List<MqttListener> result = listenerCache.query();
        // get connection counts
        Map<String, Integer> counts = connectionCache.count();

        result.forEach(listener -> {
            listener.setConnCount(counts.get(listener.getAddressAndPort()) == null ? 0 : counts.get(listener.getAddressAndPort()));
        });

        return result;
    }

    @Override
    public void saveConnection(MqttConnection connection) {
        connectionCache.save(connection);
    }

    @Override
    public void deleteConnection(String clientId) {
        connectionCache.delete(clientId);
    }

    @Override
    public boolean containConnection(String clientId) {
        return connectionCache.contains(clientId);
    }

    @Override
    public Lock getConnectionLock(String clientId) {
        return connectionCache.lock(clientId);
    }

    @Override
    public int countConnections(String clientId) {
        return connectionCache.count(clientId);
    }

    @Override
    public List<MqttConnection> getConnections(String clientId, Integer pageNum, Integer pageSize) {
        return connectionCache.query(clientId, pageNum, pageSize);
    }

    @Override
    public void initTransientSession(String clientId) {
        // do nothing...
    }

    @Override
    public void destroyTransientSession(String clientId) {
        // delete transient session routers
        deleteRouter(clientId);
    }

    @Override
    public void destroyTransientSessions(UUID nodeId) {
        // get all transient sessions
        List<MqttSession> sessions = sessionCache.get(nodeId, true);

        // check: transient sessions list is not null and not empty
        if (sessions != null && !sessions.isEmpty()) {
            // get ignite compute
            IgniteCompute compute = ignite.compute(ignite.cluster());

            // destroy all transient sessions
            compute.applyAsync(destroySessionClosure, sessions);
        }
    }

    @Override
    public void initPersistentSession(String clientId) {
        // TODO remove node id field from the mqtt router entity,
        // persistent session does not have to update node id field every time
        updateRouter(clientId);
    }

    @Override
    public void destroyPersistentSession(String clientId) {
        // delete persistent session routers
        deleteRouter(clientId);

        // close await message cache
        closeAwait(clientId);

        // close queue cache
        closeQueue(clientId);

        // close inflight message cache
        closeInflight(clientId);
    }

    @Override
    public void saveSession(MqttSession session) {
        if (CacheConst.NEW == sessionCache.save(session)) {
            // TODO if metrics disabled, should not create session quotas
            MetricsUtil.getSessionQuotas().forEach(prefix -> {
                // get or create session metrics quota
                IgniteAtomicLong quota = ignite.atomicLong(prefix + session.getClientId(), 0, true);

                if (quota != null && !quota.removed()) {
                    quota.getAndSet(0);
                }
            });
        }
    }

    @Override
    public boolean deleteSession(String clientId) {
        // TODO if metrics disabled, should not remove session quotas
        MetricsUtil.getSessionQuotas().forEach(prefix -> {
            // get session metrics quota
            IgniteAtomicLong quota = ignite.atomicLong(prefix + clientId, 0, false);

            if (quota != null && !quota.removed()) {
                quota.close();
            }
        });

        return sessionCache.delete(clientId);
    }

    @Override
    public boolean containSession(String clientId) {
        return sessionCache.contains(clientId);
    }

    @Override
    public MqttSession getSession(String clientId) {
        return sessionCache.get(clientId);
    }

    @Override
    public Lock getSessionLock(String clientId) {
        return sessionCache.lock(clientId);
    }

    @Override
    public boolean isTransientSession(String clientId) {
        MqttSession session = sessionCache.get(clientId);

        if (session != null) {
            return session.isCleanSession();
        }

        return true;
    }

    @Override
    public boolean isPersistentSession(String clientId) {
        MqttSession session = sessionCache.get(clientId);

        if (session != null) {
            return !session.isCleanSession();
        }

        return false;
    }

    @Override
    public int countSessions(UUID nodeId, String clientId) {
        return sessionCache.count(nodeId, clientId);
    }

    @Override
    public List<JsonObject> getSessions(UUID nodeId, String clientId, Integer pageNum, Integer pageSize) {
        // session json objects
        List<JsonObject> result = new ArrayList<>();
        // session json object
        JsonObject item = null;
        // session quota
        String quota = null;
        // session quota value
        IgniteAtomicLong value = null;

        // get prefixes
        List<String> prefixes = MetricsUtil.getSessionQuotas();
        // get sessions
        List<MqttSession> sessions = sessionCache.query(nodeId, clientId, pageNum, pageSize);

        if (sessions == null || sessions.isEmpty()) {
            return result;
        }

        for (MqttSession session : sessions) {
            // map from mqtt session
            item = JsonObject.mapFrom(session);
            // set session max inflight size
            item.put("maxInflightSize", Config.getSessionInflightMaxCapacity());

            // TODO if metrics disabled, should not get session quotas, all session quotas'
            // value set to N.A
            for (String prefix : prefixes) {
                // get session metrics quota name
                quota = prefix.replace(MetricsConst.SSM_PREFIX, "").replace("_", "");
                // get session metrics quota value
                value = ignite.atomicLong(prefix + session.getClientId(), 0, false);

                if (value != null && !value.removed()) {
                    item.put(quota, value.get());
                } else {
                    item.put(quota, 0L);
                }
            }

            result.add(item);
        }

        return result;
    }

    @Override
    public void saveRouter(MqttRouter router) {
        // TODO remove node id field from the mqtt router entity,
        // persistent session does not have to update node id field every time
        routerCache.save(router, thisNodeId);

        // 1.metrics: set this node topic count
        // 2.metrics: set this node topic max
        // 3.metrics: set this node subscribe count
        // 4.metrics: set this node subscribe max
        MetricsService.i().grecord(MetricsConst.GPN_TOPIC_AND_SUBSCRIBE, 0);
    }

    @Override
    public void updateRouter(String clientId) {
        // TODO remove node id field from the mqtt router entity,
        // persistent session does not have to update node id field every time
        routerCache.update(clientId, thisNodeId);

        // 1.metrics: set this node topic count
        // 2.metrics: set this node topic max
        // 3.metrics: set this node subscribe count
        // 4.metrics: set this node subscribe max
        MetricsService.i().grecord(MetricsConst.GPN_TOPIC_AND_SUBSCRIBE, 0);
    }

    @Override
    public void deleteRouter(String clientId) {
        routerCache.delete(clientId);

        // 1.metrics: set this node topic count
        // 2.metrics: set this node topic max
        // 3.metrics: set this node subscribe count
        // 4.metrics: set this node subscribe max
        MetricsService.i().grecord(MetricsConst.GPN_TOPIC_AND_SUBSCRIBE, 0);
    }

    @Override
    public void deleteRouter(String clientId, String topic) {
        routerCache.delete(clientId, topic);

        // 1.metrics: set this node topic count
        // 2.metrics: set this node topic max
        // 3.metrics: set this node subscribe count
        // 4.metrics: set this node subscribe max
        MetricsService.i().grecord(MetricsConst.GPN_TOPIC_AND_SUBSCRIBE, 0);
    }

    @Override
    public MqttRouter getRouter(String clientId, String topic) {
        return routerCache.get(clientId, topic);
    }

    @Override
    public List<MqttRoute> getRoutes(String topic, int qos) {
        return routerCache.getRoutes(topic, qos);
    }

    @Override
    public abstract List<MqttRoute> getClusteredRoutes(String topic, int qos);

    @Override
    public int countTopics(UUID nodeId, String topic) {
        return routerCache.countTopics(nodeId, topic);
    }

    @Override
    public List<MqttRouter> getTopics(UUID nodeId, String topic, Integer pageNum, Integer pageSize) {
        return routerCache.queryTopics(nodeId, topic, pageNum, pageSize);
    }

    @Override
    public int countSubscribes(UUID nodeId, String clientId) {
        return routerCache.countSubscribes(nodeId, clientId);
    }

    @Override
    public List<MqttRouter> getSubscribes(UUID nodeId, String clientId, Integer pageNum, Integer pageSize) {
        return routerCache.querySubscribes(nodeId, clientId, pageNum, pageSize);
    }

    @Override
    public BoundedIgniteMap<MqttAwaitMessageKey, MqttAwaitMessage> openAwait(String clientId) {
        return awaitCache.open(clientId);
    }

    @Override
    public void closeAwait(String clientId) {
        awaitCache.close(clientId);
    }

    @Override
    public BoundedIgniteMapUnsafe<MqttMessageKey, MqttMessage> openInflight(String clientId) {
        return inflightCache.open(clientId);
    }

    @Override
    public void closeInflight(String clientId) {
        inflightCache.close(clientId);
    }

    @Override
    public BoundedIgniteQueue<MqttMessage> openQueue(String clientId) {
        return queueCache.open(clientId);
    }

    @Override
    public void closeQueue(String clientId) {
        queueCache.close(clientId);
    }

    @Override
    public boolean enqueue(MqttPublishMessage message, String clientId, int qos) {
        return queueCache.enqueue(message, clientId, qos);
    }

    @Override
    public boolean enqueue(MqttMessage message, String clientId, int qos) {
        return queueCache.enqueue(message, clientId, qos);
    }

    @Override
    public void saveRetain(MqttRetainMessage message) {
        retainCache.save(message);
    }

    @Override
    public void deleteRetain(String topic) {
        retainCache.delete(topic);
    }

    @Override
    public List<MqttRetainMessage> getRetains(String topic, int qos) {
        return retainCache.get(topic, qos);
    }

    @Override
    public int countRetains() {
        return retainCache.size();
    }

    @Override
    public void saveWill(MqttWillMessage will) {
        willCache.save(will);
    }

    @Override
    public void deleteWill(String clientId, boolean force) {
        // if is force
        if (force) {
            // delete will message
            willCache.delete(clientId);
        } else {
            // get will message
            MqttWillMessage will = willCache.get(clientId);

            // check: will message is not retain
            if (will != null && !will.isRetain()) {
                // delete will message
                willCache.delete(clientId);
            }
        }
    }

    @Override
    public MqttWillMessage getWill(String clientId) {
        return willCache.get(clientId);
    }

    @Override
    public int addUser(MqttUser user) {
        return userCache.add(user);
    }

    @Override
    public int deleteUser(String username) {
        return userCache.delete(username);
    }

    @Override
    public int updateUser(MqttUser user, String adminAccount, String adminPasswd) {
        if (user != null && StringUtils.isNotBlank(user.getPassword())) {
            return userCache.update(user, adminAccount, adminPasswd);
        } else {
            return userCache.update(user);
        }
    }

    @Override
    public int countUsers(String username) {
        return userCache.count(username);
    }

    @Override
    public List<MqttUser> getUsers(String username, Integer pageNum, Integer pageSize) {
        return userCache.query(username, pageNum, pageSize);
    }

    @Override
    public MqttUser getUser(String username) {
        return userCache.get(username);
    }

    @Override
    public long addAcl(MqttAcl acl) {
        return aclCache.add(acl);
    }

    @Override
    public int deleteAcl(Long seq) {
        return aclCache.delete(seq);
    }

    @Override
    public int updateAcl(MqttAcl acl) {
        return aclCache.update(acl);
    }

    @Override
    public int reorderAcls(List<MqttAcl> list) {
        return aclCache.reorder(list);
    }

    @Override
    public List<Object[]> getAcls() {
        return aclCache.query();
    }

    @Override
    public List<MqttAcl> getAcls(String username, String ipAddr, String clientId) {
        return aclCache.get(username, ipAddr, clientId);
    }

    @Override
    public int addAdmin(MqttAdmin admin) {
        return adminCache.add(admin);
    }

    @Override
    public int deleteAdmin(String account) {
        return adminCache.delete(account);
    }

    @Override
    public int updateAdmin(MqttAdmin admin, String oldPasswd, String newPasswd) {
        if (StringUtils.isNotBlank(oldPasswd) && StringUtils.isNotBlank(newPasswd)) {
            return adminCache.update(admin, oldPasswd, newPasswd);
        } else {
            return adminCache.update(admin);
        }
    }

    @Override
    public int countAdmins(String account) {
        return adminCache.count(account);
    }

    @Override
    public List<MqttAdmin> getAdmins(String account, Integer pageNum, Integer pageSize) {
        return adminCache.query(account, pageNum, pageSize);
    }

    @Override
    public boolean login(MqttAdmin admin) {
        if (admin == null) {
            return false;
        }

        String account = admin.getAccount();
        String password = admin.getPassword();

        if (StringUtils.isBlank(account) || StringUtils.isBlank(password)) {
            return false;
        }

        MqttAdmin compared = adminCache.get(account);

        if (compared == null) {
            return false;
        } else {
            return AesUtil.encryptBase64(password).equals(compared.getPassword());
        }
    }

}
