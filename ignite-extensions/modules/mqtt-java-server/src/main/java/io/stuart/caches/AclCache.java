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

package io.stuart.caches;

import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.impl.AclCacheImpl;
import io.stuart.entities.auth.MqttAcl;

public interface AclCache {

    static AclCache create(Ignite ignite, CacheConfiguration<Long, MqttAcl> cfg) {
        return new AclCacheImpl(ignite, cfg);
    }

    void init();

    long add(MqttAcl acl);

    int delete(Long seq);

    int update(MqttAcl acl);

    int reorder(List<MqttAcl> acls);

    List<MqttAcl> get(String username, String ipAddr, String clientId);

    List<Object[]> query();

}
