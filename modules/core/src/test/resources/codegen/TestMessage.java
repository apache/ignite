/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.lang.String;
import java.util.UUID;
import java.util.BitSet;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;

public class TestMessage implements Message {
    @Order(0)
    int id;

    @Order(1)
    byte[] byteArr;

    @Order(2)
    String str;

    @Order(3)
    String[] strArr;

    @Order(4)
    int[][] intMatrix;

    @Order(5)
    GridCacheVersion ver;

    @Order(6)
    GridCacheVersion[] verArr;

    @Order(7)
    UUID uuid;

    @Order(8)
    IgniteUuid ignUuid;

    @Order(9)
    AffinityTopologyVersion topVer;

    @Order(10)
    BitSet bitSet;

    @Order(value = 11, method = "overridenFieldMethod")
    private String field;

    @Order(12)
    KeyCacheObject keyCacheObject;

    @Order(13)
    CacheObject cacheObject;

    @Order(14)
    GridLongList gridLongList;

    public String overridenFieldMethod() {
        return field;
    }

    public void overridenFieldMethod(String field) {
        this.field = field;
    }

    public short directType() {
        return 0;
    }
}
