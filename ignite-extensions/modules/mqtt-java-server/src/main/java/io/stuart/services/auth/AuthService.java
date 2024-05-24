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

package io.stuart.services.auth;

import java.util.List;
import java.util.function.Function;

import io.stuart.entities.internal.MqttAuthority;
import io.stuart.services.PowerService;

public interface AuthService extends PowerService {

    void auth(String username, String password, Function<Boolean, Void> handler);

    void access(String username, String ipAddr, String clientId, final List<MqttAuthority> auth, Function<List<MqttAuthority>, Void> handler);

    void access(String username, String ipAddr, String clientId, MqttAuthority auth, Function<MqttAuthority, Void> handler);

}
