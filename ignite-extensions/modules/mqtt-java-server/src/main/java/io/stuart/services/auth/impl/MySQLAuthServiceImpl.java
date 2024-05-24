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

import io.vertx.ext.jdbc.JDBCClient;
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

import io.vertx.ext.sql.ResultSet;

public class MySQLAuthServiceImpl implements AuthService {

    private static final String queryAcl;

    private Vertx vertx;

    private JDBCClient mysql;

    static {
        queryAcl = queryAcl();
    }

    private static String queryAcl() {
        StringBuilder sql = new StringBuilder();

        sql.append(" select acl.topic, acl.authority from stuart_acl acl where ");
        sql.append(" (acl.target = ? and acl.type = ?) or (acl.target = ? and acl.type = ?) or ");
        sql.append(" (acl.target = ? and acl.type = ?) or (acl.target = ? and acl.type = ?) ");
        sql.append(" order by acl.seq asc ");

        return sql.toString();
    }

    public MySQLAuthServiceImpl(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start() {
        Logger.log().info("Stuart's mysql authentication service is starting...");

        JsonObject config = new JsonObject();

        config.put("host", Config.getAuthRdbHost());
        config.put("port", Config.getAuthRdbPort());
        config.put("username", Config.getAuthRdbUsername());
        config.put("password", Config.getAuthRdbPassword());
        config.put("database", Config.getAuthRdbDatabase());
        config.put("charset", Config.getAuthRdbCharset());
        config.put("maxPoolSize", Config.getAuthRdbMaxPoolSize());
        config.put("queryTimeout", Config.getAuthRdbQueryTimeoutMs());

        mysql = JDBCClient.createShared(vertx, config);

        Logger.log().info("Stuart's mysql authentication service start succeeded.");
    }

    @Override
    public void stop() {
        if (mysql == null) {
            return;
        }

        mysql.close(ar -> {
            if (ar.succeeded()) {
                Logger.log().info("Stuart's mysql authentication service close succeeded.");
            } else {
                Logger.log().error("Stuart's mysql authentication service close failed, exception: {}.", ar.cause().getMessage());
            }
        });
    }

    @Override
    public void auth(String username, String password, Function<Boolean, Void> handler) {
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            handler.apply(false);
        } else {
            String sql = "select password from stuart_user where username = ?";
            JsonArray args = new JsonArray().add(username);

            mysql.queryWithParams(sql, args, ar -> {
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
        JsonArray args = new JsonArray();
        args.add(username);
        args.add(Target.Username.value());
        args.add(ipAddr);
        args.add(Target.IpAddr.value());
        args.add(clientId);
        args.add(Target.ClientId.value());
        args.add(AclConst.ALL);
        args.add(Target.All.value());

        mysql.queryWithParams(queryAcl, args, ar -> {
            if (ar.succeeded()) {
                setAuthority(auths, ar.result());
            }

            handler.apply(auths);
        });
    }

    @Override
    public void access(String username, String ipAddr, String clientId, final MqttAuthority auth, Function<MqttAuthority, Void> handler) {
        JsonArray args = new JsonArray();
        args.add(username);
        args.add(Target.Username.value());
        args.add(ipAddr);
        args.add(Target.IpAddr.value());
        args.add(clientId);
        args.add(Target.ClientId.value());
        args.add(AclConst.ALL);
        args.add(Target.All.value());

        mysql.queryWithParams(queryAcl, args, ar -> {
            if (ar.succeeded()) {
                setAuthority(auth, ar.result());
            }

            handler.apply(auth);
        });
    }

    private boolean passwdEquals(String password, ResultSet rs) {
        if (rs == null) {
            return false;
        }

        List<JsonArray> results = rs.getResults();

        if (results == null || results.isEmpty()) {
            return false;
        }

        String enPasswd = Config.getAes().encryptBase64(password);
        String qyPasswd = results.get(0).getString(0);

        if (qyPasswd != null && enPasswd.equals(qyPasswd)) {
            return true;
        } else {
            return false;
        }
    }

    private void setAuthority(final List<MqttAuthority> auths, ResultSet rs) {
        if (rs == null) {
            return;
        }

        List<JsonArray> acls = rs.getResults();

        if (acls == null || acls.isEmpty()) {
            return;
        }

        for (MqttAuthority auth : auths) {
            // qos is 0x80
            if (MqttQoS.FAILURE.value() == auth.getQos()) {
                // next one
                continue;
            }

            // set access and authority
            setAuthority(auth, rs);
        }
    }

    private void setAuthority(final MqttAuthority auth, ResultSet rs) {
        if (rs == null) {
            return;
        }

        List<JsonArray> acls = rs.getResults();

        if (acls == null || acls.isEmpty()) {
            return;
        }

        // get topic
        String topic = auth.getTopic();
        // transformed authority from MySQL
        MqttAuthority transformed = null;

        for (JsonArray acl : acls) {
            // is match
            if (AuthUtil.isMatch(topic, acl.getString(0))) {
                // get transformed authority
                transformed = AuthUtil.transform2Authority(acl.getString(1));

                // set access
                auth.setAccess(transformed.getAccess());
                // set authority
                auth.setAuthority(transformed.getAuthority());

                // break
                break;
            }
        }
    }

}
