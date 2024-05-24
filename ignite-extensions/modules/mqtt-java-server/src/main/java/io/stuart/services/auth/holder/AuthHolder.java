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

package io.stuart.services.auth.holder;

import io.stuart.config.Config;
import io.stuart.consts.ParamConst;
import io.stuart.services.auth.AuthService;
import io.stuart.services.auth.impl.LocalAuthServiceImpl;
import io.stuart.services.auth.impl.MongoDBAuthServiceImpl;
import io.stuart.services.auth.impl.MySQLAuthServiceImpl;
import io.stuart.services.auth.impl.RedisAuthServiceImpl;
import io.stuart.services.cache.CacheService;
import io.vertx.core.Vertx;

public class AuthHolder {

    private static volatile AuthService authService;

    public static AuthService getAuthService(Vertx vertx, CacheService cacheService) {
        if (authService == null) {
            synchronized (AuthHolder.class) {
                if (authService == null) {
                    // get authentication mode
                    String authMode = Config.getAuthMode();

                    if (ParamConst.AUTH_MODE_REDIS.equalsIgnoreCase(authMode)) {
                        // new redis authentication and authorization service
                        authService = new RedisAuthServiceImpl(vertx);
                    } else if (ParamConst.AUTH_MODE_MYSQL.equalsIgnoreCase(authMode)) {
                        // new mysql authentication and authorization service
                        authService = new MySQLAuthServiceImpl(vertx);
                    } else if (ParamConst.AUTH_MODE_MONGO.equalsIgnoreCase(authMode)) {
                        // new mongodb authentication and authorization service
                        authService = new MongoDBAuthServiceImpl(vertx);
                    } else if (ParamConst.AUTH_MODE_LOCAL.equalsIgnoreCase(authMode)) {
                        // new local authentication and authorization service
                        authService = new LocalAuthServiceImpl(cacheService);
                    } else {
                        // set authentication and authorization service = null
                        authService = null;
                    }
                }
            }
        }

        return authService;
    }

}
