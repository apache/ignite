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

package io.stuart.entities.auth;

import java.io.Serializable;

import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
@QueryGroupIndex(name = "mqtt_acl_dx", inlineSize = -1)
public class MqttAcl implements Serializable {

    private static final long serialVersionUID = 8183942094437614770L;

    @QuerySqlField(index = true, inlineSize = -1)
    private long seq;

    @QuerySqlField(groups = { "mqtt_acl_dx" })
    private String target;

    @QuerySqlField(groups = { "mqtt_acl_idx" })
    private int type;

    @QuerySqlField
    private String topic;

    @QuerySqlField
    private int access;

    @QuerySqlField
    private int authority;

    private String createAccount;

    private long createTime;

    private String updateAccount;

    private long updateTime;

}
