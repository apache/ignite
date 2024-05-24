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

package io.stuart.services.cache;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import org.apache.ignite.Ignite;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;

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
import io.stuart.services.PowerService;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.messages.MqttPublishMessage;

public interface CacheService extends PowerService {

    Ignite getIgnite();

    void eventLocalListen(IgnitePredicate<? extends Event> listener, int... types);

    <R> R computeCall(UUID nodeId, IgniteCallable<R> job);

    <R, T> R computeApply(UUID nodeId, IgniteClosure<T, R> job, T arg);

    void initNode(boolean firstNode);

    void updateNodeSysRuntimeInfo();

    UUID localNodeId();

    Set<UUID> remoteNodeIds();

    boolean isLocalAuth(UUID nodeId);

    MqttNode getNode(UUID nodeId);

    List<MqttNode> getNodes(Status status);

    void saveListener(MqttListener listener);

    List<MqttListener> getListeners();

    void saveConnection(MqttConnection connection);

    void deleteConnection(String clientId);

    boolean containConnection(String clientId);

    Lock getConnectionLock(String clientId);

    int countConnections(String clientId);

    List<MqttConnection> getConnections(String clientId, Integer pageNum, Integer pageSize);

    void initTransientSession(String clientId);

    void destroyTransientSession(String clientId);

    void destroyTransientSessions(UUID nodeId);

    void initPersistentSession(String clientId);

    void destroyPersistentSession(String clientId);

    void saveSession(MqttSession session);

    boolean deleteSession(String clientId);

    boolean containSession(String clientId);

    MqttSession getSession(String clientId);

    Lock getSessionLock(String clientId);

    boolean isTransientSession(String clientId);

    boolean isPersistentSession(String clientId);

    int countSessions(UUID nodeId, String clientId);

    List<JsonObject> getSessions(UUID nodeId, String clientId, Integer pageNum, Integer pageSize);

    void saveRouter(MqttRouter router);

    void updateRouter(String clientId);

    void deleteRouter(String clientId);

    void deleteRouter(String clientId, String topic);

    MqttRouter getRouter(String clientId, String topic);

    List<MqttRoute> getRoutes(String topic, int qos);

    List<MqttRoute> getClusteredRoutes(String topic, int qos);

    int countTopics(UUID nodeId, String topic);

    List<MqttRouter> getTopics(UUID nodeId, String topic, Integer pageNum, Integer pageSize);

    int countSubscribes(UUID nodeId, String clientId);

    List<MqttRouter> getSubscribes(UUID nodeId, String clientId, Integer pageNum, Integer pageSize);

    BoundedIgniteMap<MqttAwaitMessageKey, MqttAwaitMessage> openAwait(String clientId);

    void closeAwait(String clientId);

    BoundedIgniteMapUnsafe<MqttMessageKey, MqttMessage> openInflight(String clientId);

    void closeInflight(String clientId);

    BoundedIgniteQueue<MqttMessage> openQueue(String clientId);

    void closeQueue(String clientId);

    boolean enqueue(MqttPublishMessage message, String clientId, int qos);

    boolean enqueue(MqttMessage message, String clientId, int qos);

    void saveRetain(MqttRetainMessage message);

    void deleteRetain(String topic);

    List<MqttRetainMessage> getRetains(String topic, int qos);

    int countRetains();

    void saveWill(MqttWillMessage will);

    void deleteWill(String clientId, boolean force);

    MqttWillMessage getWill(String clientId);

    int addUser(MqttUser user);

    int deleteUser(String username);

    int updateUser(MqttUser user, String adminAccount, String adminPasswd);

    int countUsers(String username);

    List<MqttUser> getUsers(String username, Integer pageNum, Integer pageSize);

    MqttUser getUser(String username);

    long addAcl(MqttAcl acl);

    int deleteAcl(Long seq);

    int updateAcl(MqttAcl acl);

    int reorderAcls(List<MqttAcl> list);

    List<Object[]> getAcls();

    List<MqttAcl> getAcls(String username, String ipAddr, String clientId);

    int addAdmin(MqttAdmin admin);

    int deleteAdmin(String account);

    int updateAdmin(MqttAdmin admin, String oldPasswd, String newPasswd);

    int countAdmins(String account);

    List<MqttAdmin> getAdmins(String account, Integer pageNum, Integer pageSize);

    boolean login(MqttAdmin admin);

}
