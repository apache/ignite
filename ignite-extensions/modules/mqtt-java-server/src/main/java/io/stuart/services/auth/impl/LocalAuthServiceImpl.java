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
import io.stuart.entities.auth.MqttAcl;
import io.stuart.entities.auth.MqttUser;
import io.stuart.entities.internal.MqttAuthority;
import io.stuart.enums.Access;
import io.stuart.enums.Authority;
import io.stuart.services.auth.AuthService;
import io.stuart.services.cache.CacheService;
import io.stuart.utils.AuthUtil;

public class LocalAuthServiceImpl implements AuthService {

    private CacheService cacheService;

    public LocalAuthServiceImpl(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void start() {
        // do nothing...
    }

    @Override
    public void stop() {
        // do nothing...
    }

    @Override
    public void auth(String username, String password, Function<Boolean, Void> handler) {
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            handler.apply(false);
        } else {
            String enPasswd = Config.getAes().encryptBase64(password);

            MqttUser user = cacheService.getUser(username);

            if (user != null && enPasswd.equals(user.getPassword())) {
                handler.apply(true);
            } else {
                handler.apply(false);
            }
        }
    }

    @Override
    public void access(String username, String ipAddr, String clientId, final List<MqttAuthority> auths, Function<List<MqttAuthority>, Void> handler) {
        List<MqttAcl> acls = cacheService.getAcls(username, ipAddr, clientId);

        for (MqttAuthority auth : auths) {
            // qos is 0x80
            if (MqttQoS.FAILURE.value() == auth.getQos()) {
                // next one
                continue;
            }

            // set access and authority
            setAuthority(auth, acls);
        }

        handler.apply(auths);
    }

    @Override
    public void access(String username, String ipAddr, String clientId, final MqttAuthority auth, Function<MqttAuthority, Void> handler) {
        // set access and authority
        setAuthority(auth, cacheService.getAcls(username, ipAddr, clientId));

        handler.apply(auth);
    }

    private void setAuthority(final MqttAuthority auth, List<MqttAcl> acls) {
        if (acls == null || acls.isEmpty()) {
            return;
        }

        String topic = auth.getTopic();

        for (MqttAcl acl : acls) {
            // check match
            if (AuthUtil.isMatch(topic, acl.getTopic())) {
                // set access
                auth.setAccess(Access.valueOf(acl.getAccess()));
                // set authority
                auth.setAuthority(Authority.valueOf(acl.getAuthority()));

                // break
                break;
            }
        }
    }

}
