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

package io.stuart.services.auth.impl;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.config.Config;
import io.stuart.consts.AclConst;
import io.stuart.entities.internal.MqttAuthority;
import io.stuart.enums.Target;
import io.stuart.log.Logger;
import io.stuart.services.auth.AuthService;
import io.stuart.utils.AuthUtil;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

public class MongoDBAuthServiceImpl implements AuthService {

    private Vertx vertx;

    private MongoClient mongo;

    public MongoDBAuthServiceImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start() {
        Logger.log().info("Stuart's mongodb authentication service is starting...");

        JsonObject config = new JsonObject();

        config.put("host", Config.getAuthMongoHost());
        config.put("port", Config.getAuthMongoPort());
        config.put("db_name", Config.getAuthMongoDbName());
        config.put("maxPoolSize", Config.getAuthMongoMaxPoolSize());
        config.put("minPoolSize", Config.getAuthMongoMinPoolSize());
        config.put("maxIdleTimeMS", Config.getAuthMongoMaxIdleTimeMs());
        config.put("maxLifeTimeMS", Config.getAuthMongoMaxLifeTimeMs());
        config.put("waitQueueMultiple", Config.getAuthMongoWaitQueueMultiple());
        config.put("waitQueueTimeoutMS", Config.getAuthMongoWaitQueueTimeoutMs());
        config.put("maintenanceFrequencyMS", Config.getAuthMongoMaintenanceFrequencyMs());
        config.put("maintenanceInitialDelayMS", Config.getAuthMongoMaintenanceInitialDelayMs());
        config.put("connectTimeoutMS", Config.getAuthMongoConnectTimeoutMs());
        config.put("socketTimeoutMS", Config.getAuthMongoSocketTimeoutMs());

        if (StringUtils.isNotBlank(Config.getAuthMongoUsername())) {
            config.put("username", Config.getAuthMongoUsername());
        }

        if (StringUtils.isNotBlank(Config.getAuthMongoPassword())) {
            config.put("password", Config.getAuthMongoPassword());
        }

        if (StringUtils.isNotBlank(Config.getAuthMongoAuthSource())) {
            config.put("authSource", Config.getAuthMongoAuthSource());
        }

        if (StringUtils.isNotBlank(Config.getAuthMongoAuthMechanism())) {
            config.put("authMechanism", Config.getAuthMongoAuthMechanism());
        }

        mongo = MongoClient.createShared(vertx, config);

        Logger.log().info("Stuart's mongodb authentication service start succeeded.");
    }

    @Override
    public void stop() {
        if (mongo == null) {
            return;
        }

        mongo.close();

        Logger.log().info("Stuart's mongodb authentication service close finished.");
    }

    @Override
    public void auth(String username, String password, Function<Boolean, Void> handler) {
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            handler.apply(false);
        } else {
            JsonObject query = new JsonObject();

            query.put(Config.getAuthMongoUserUsernameField(), username);

            mongo.find(Config.getAuthMongoUser(), query, ar -> {
                if (ar.succeeded() && passwdEquals(password, ar.result())) {
                    handler.apply(true);
                } else {
                    handler.apply(false);
                }
            });
        }
    }

    @Override
    public void access(String username, String ipAddr, String clientId, final List<MqttAuthority> auths, Function<List<MqttAuthority>, Void> handler) {
        // or query conditions
        JsonArray or = new JsonArray();
        // add query conditions
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), username).put(Config.getAuthMongoAclTypeField(), Target.Username.value()));
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), ipAddr).put(Config.getAuthMongoAclTypeField(), Target.IpAddr.value()));
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), clientId).put(Config.getAuthMongoAclTypeField(), Target.ClientId.value()));
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), AclConst.ALL).put(Config.getAuthMongoAclTypeField(), Target.All.value()));

        // set $or key and value
        JsonObject query = new JsonObject().put("$or", or);

        // find option
        FindOptions options = new FindOptions();
        // set sort
        options.setSort(new JsonObject().put(Config.getAuthMongoAclSeqField(), 1));

        mongo.findWithOptions(Config.getAuthMongoAcl(), query, options, ar -> {
            if (ar.succeeded()) {
                setAuthority(auths, ar.result());
            }

            handler.apply(auths);
        });
    }

    @Override
    public void access(String username, String ipAddr, String clientId, final MqttAuthority auth, Function<MqttAuthority, Void> handler) {
        // or query conditions
        JsonArray or = new JsonArray();
        // add query conditions
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), username).put(Config.getAuthMongoAclTypeField(), Target.Username.value()));
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), ipAddr).put(Config.getAuthMongoAclTypeField(), Target.IpAddr.value()));
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), clientId).put(Config.getAuthMongoAclTypeField(), Target.ClientId.value()));
        or.add(new JsonObject().put(Config.getAuthMongoAclTargetField(), AclConst.ALL).put(Config.getAuthMongoAclTypeField(), Target.All.value()));

        // set $or key and value
        JsonObject query = new JsonObject().put("$or", or);

        // find option
        FindOptions options = new FindOptions();
        // set sort
        options.setSort(new JsonObject().put(Config.getAuthMongoAclSeqField(), 1));

        mongo.findWithOptions(Config.getAuthMongoAcl(), query, options, ar -> {
            if (ar.succeeded()) {
                setAuthority(auth, ar.result());
            }

            handler.apply(auth);
        });
    }

    private boolean passwdEquals(String password, List<JsonObject> rs) {
        if (rs == null) {
            return false;
        }

        if (rs.isEmpty() || rs.size() > 1) {
            return false;
        }

        String enPasswd = Config.getAes().encryptBase64(password);
        String qyPasswd = rs.get(0).getString(Config.getAuthMongoUserPasswordField());

        if (qyPasswd != null && enPasswd.equals(qyPasswd)) {
            return true;
        } else {
            return false;
        }
    }

    private void setAuthority(final List<MqttAuthority> auths, List<JsonObject> rs) {
        if (rs == null || rs.isEmpty()) {
            return;
        }

        for (MqttAuthority auth : auths) {
            // qos is 0x80
            if (MqttQoS.FAILURE.value() == auth.getQos()) {
                // next one
                continue;
            }

            // set authority
            setAuthority(auth, rs);
        }
    }

    private void setAuthority(final MqttAuthority auth, List<JsonObject> rs) {
        if (rs == null || rs.isEmpty()) {
            return;
        }

        // get topic
        String topic = auth.getTopic();
        // transformed authority from MongoDB
        MqttAuthority transformed = null;

        // query result's topics field
        JsonArray topics = null;
        // each topic of query result's topics field
        JsonObject filter = null;
        // the topics field size of each query result
        int size = 0;

        for (JsonObject res : rs) {
            topics = res.getJsonArray(Config.getAuthMongoAclTopicsField());

            if (topics == null || topics.isEmpty()) {
                continue;
            }

            size = topics.size();

            for (int i = 0; i < size; ++i) {
                // get topic
                filter = topics.getJsonObject(i);

                // if matched
                if (AuthUtil.isMatch(topic, filter.getString(Config.getAuthMongoAclTopicField()))) {
                    // get transformed authority
                    transformed = AuthUtil.transform2Authority(filter.getString(Config.getAuthMongoAclAuthorityField()));

                    // set access
                    auth.setAccess(transformed.getAccess());
                    // set authority
                    auth.setAuthority(transformed.getAuthority());

                    // break the loop
                    break;
                }
            }

            if (auth.getAccess() != null && auth.getAuthority() != null) {
                break;
            }
        }
    }

}
