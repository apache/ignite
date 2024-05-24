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

package io.stuart.entities.cache;

import java.io.Serializable;
import java.util.UUID;

import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
@QueryGroupIndex(name = "mqtt_session_idx", inlineSize = -1)
public class MqttSession implements Serializable {

    private static final long serialVersionUID = -8008170929831234461L;

    @QuerySqlField(groups = { "mqtt_session_idx" })
    private UUID nodeId;

    @QuerySqlField(groups = { "mqtt_session_idx" })
    private String clientId;

    @QuerySqlField(groups = { "mqtt_session_idx" })
    private boolean cleanSession;

    @QuerySqlField
    private long createTime;

}
