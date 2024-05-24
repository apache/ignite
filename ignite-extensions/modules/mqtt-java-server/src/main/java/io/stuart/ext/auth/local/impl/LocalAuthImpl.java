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

package io.stuart.ext.auth.local.impl;

import io.stuart.entities.auth.MqttAdmin;
import io.stuart.ext.auth.local.LocalAuth;
import io.stuart.services.cache.CacheService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.impl.UserImpl;

public class LocalAuthImpl implements AuthProvider, LocalAuth {

    private CacheService cacheService;

    public LocalAuthImpl(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public void authenticate(JsonObject authInfo, Handler<AsyncResult<User>> resultHandler) {
        String username = authInfo.getString("username");

        if (username == null) {
            resultHandler.handle(Future.failedFuture("authInfo must contain username in 'username' field"));
            return;
        }

        String password = authInfo.getString("password");

        if (password == null) {
            resultHandler.handle(Future.failedFuture("authInfo must contain password in 'password' field"));
            return;
        }

        MqttAdmin admin = new MqttAdmin();
        admin.setAccount(username);
        admin.setPassword(password);

        JsonObject principal = new JsonObject();
        principal.put("username",username);

        JsonObject attr = new JsonObject();
        attr.put("username",username);
        if (cacheService.login(admin)) {
            resultHandler.handle(Future.succeededFuture(User.create(principal, attr)));
        } else {
            resultHandler.handle(Future.failedFuture("no such username, or password incorrect."));
        }
    }

}
