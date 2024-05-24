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
import java.util.Map;
import java.util.function.Function;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.redis.client.*;
import io.vertx.redis.client.impl.RedisClient;
import io.vertx.redis.client.impl.RedisURI;
import org.apache.commons.lang3.StringUtils;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.config.Config;
import io.stuart.consts.AclConst;
import io.stuart.entities.internal.MqttAuthority;
import io.stuart.enums.Target;
import io.stuart.log.Logger;
import io.stuart.services.auth.AuthService;
import io.stuart.utils.AuthUtil;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;


public class RedisAuthServiceImpl implements AuthService {

    private Vertx vertx;

    private RedisAPI redis;

    public RedisAuthServiceImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start() {
        Logger.log().info("Stuart's redis authentication service is starting...");

        RedisOptions cfg = new RedisOptions();

        cfg.setConnectionString("redis://"+Config.getAuthRedisHost()+':'+Config.getAuthRedisPort()+"/"+Config.getAuthRedisSelect());

        cfg.setType(RedisClientType.STANDALONE);

        if (StringUtils.isNotBlank(Config.getAuthRedisPass())) {
            cfg.setPassword(Config.getAuthRedisPass());
        }

        Redis client = Redis.createClient(vertx, cfg);

        redis = RedisAPI.api(client);

        Logger.log().info("Stuart's redis authentication service start succeeded.");
    }

    @Override
    public void stop() {
        if (redis == null) {
            return;
        }
        redis.close();
    }

    @Override
    public void auth(String username, String password, Function<Boolean, Void> handler) {
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            handler.apply(false);
        } else {
            String queryKey = Config.getAuthRedisUserKeyPrefix() + username;
            String enPasswd = Config.getAes().encryptBase64(password);

            redis.hget(queryKey, Config.getAuthRedisPasswdField(), ar -> {
                if (ar.succeeded() && enPasswd.equals(ar.result())) {
                    handler.apply(true);
                } else {
                    handler.apply(false);
                }
            });
        }
    }

    @Override
    public void access(String username, String ipAddr, String clientId, final List<MqttAuthority> auths, Function<List<MqttAuthority>, Void> handler) {
        String userKey = transform2Key(username, Target.Username);
        String ipAddrKey = transform2Key(ipAddr, Target.IpAddr);
        String clientKey = transform2Key(clientId, Target.ClientId);
        String allKey = transform2Key(AclConst.ALL, Target.All);

        Future<JsonObject> userFut = Future.future(x -> {});
        Future<JsonObject> ipAddrFut = Future.future(x -> {});
        Future<JsonObject> clientFut = Future.future(x -> {});
        Future<JsonObject> allFut = Future.future(x -> {});

        redis.hgetall(userKey, (AsyncResult<Response> e) -> { userFut.map(e); });
        redis.hgetall(ipAddrKey, (AsyncResult<Response> e) -> { ipAddrFut.map(e); });
        redis.hgetall(clientKey, (AsyncResult<Response> e) -> { clientFut.map(e); });
        redis.hgetall(allKey, (AsyncResult<Response> e) -> { allFut.map(e); });

        CompositeFuture.join(userFut, ipAddrFut, clientFut, allFut).onSuccess(ar -> {
            if (ar.succeeded()) {
                setAuthority(auths, ar.result().<JsonObject>list());
            }

            handler.apply(auths);
        });
    }

    @Override
    public void access(String username, String ipAddr, String clientId, final MqttAuthority auth, Function<MqttAuthority, Void> handler) {
        String userKey = transform2Key(username, Target.Username);
        String ipAddrKey = transform2Key(ipAddr, Target.IpAddr);
        String clientKey = transform2Key(clientId, Target.ClientId);
        String allKey = transform2Key(AclConst.ALL, Target.All);

        Future<JsonObject> userFut = Future.future(x -> {});
        Future<JsonObject> ipAddrFut = Future.future(x -> {});
        Future<JsonObject> clientFut = Future.future(x -> {});
        Future<JsonObject> allFut = Future.future(x -> {});

        redis.hgetall(userKey, (AsyncResult<Response> e) -> { userFut.map(e); });
        redis.hgetall(ipAddrKey, (AsyncResult<Response> e) -> { ipAddrFut.map(e); });
        redis.hgetall(clientKey, (AsyncResult<Response> e) -> { clientFut.map(e); });
        redis.hgetall(allKey, (AsyncResult<Response> e) -> { allFut.map(e); });

        CompositeFuture.join(userFut, ipAddrFut, clientFut, allFut).onSuccess(ar -> {
            if (ar.succeeded()) {
                setAuthority(auth, ar.result().<JsonObject>list());
            }

            handler.apply(auth);
        });
    }

    private String transform2Key(String target, Target type) {
        String prefix = null;

        if (Target.Username == type) {
            prefix = Config.getAuthRedisAclUserKeyPrefix();
        } else if (Target.IpAddr == type) {
            prefix = Config.getAuthRedisAclIpAddrKeyPrefix();
        } else if (Target.ClientId == type) {
            prefix = Config.getAuthRedisAclClientKeyPrefix();
        } else {
            prefix = Config.getAuthRedisAclAllKeyPrefix();
        }

        return prefix + target;
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

            setAuthority(auth, rs);
        }
    }

    private void setAuthority(final MqttAuthority auth, List<JsonObject> rs) {
        if (rs == null || rs.isEmpty()) {
            return;
        }

        // get topic
        String topic = auth.getTopic();
        // transformed authority from MySQL
        MqttAuthority transformed = null;

        for (JsonObject res : rs) {
            // loop
            for (Map.Entry<String, Object> entry : res) {
                // if matched
                if (AuthUtil.isMatch(topic, entry.getKey())) {
                    // get transformed authority
                    transformed = AuthUtil.transform2Authority(entry.getValue().toString());

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
