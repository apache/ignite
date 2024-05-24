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

package io.stuart.closures;

import org.apache.ignite.lang.IgniteClosure;

import io.stuart.entities.internal.MqttConnections;
import io.stuart.entities.param.QueryConnections;
import io.stuart.services.cache.CacheService;

public class QueryConnectionsClosure implements IgniteClosure<QueryConnections, MqttConnections> {

    private static final long serialVersionUID = 278244255482949257L;

    private CacheService cacheService;

    public QueryConnectionsClosure(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public MqttConnections apply(QueryConnections qcp) {
        String clientId = qcp.getClientId();
        int pageNum = qcp.getPageNum();
        int pageSize = qcp.getPageSize();

        MqttConnections conns = new MqttConnections();
        conns.setTotal(cacheService.countConnections(clientId));
        conns.setConnections(cacheService.getConnections(clientId, pageNum, pageSize));

        return conns;
    }

}
