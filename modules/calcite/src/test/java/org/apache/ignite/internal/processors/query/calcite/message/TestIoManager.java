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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class TestIoManager {
    /** */
    private final Map<UUID, TestMessageService> srvcMap = new ConcurrentHashMap<>();

    /** */
    protected void send(UUID senderId, UUID nodeId, CalciteMessage msg) {
        TestMessageService target = srvcMap.get(nodeId);

        assert target != null;

        target.onMessage(senderId, msg, true);
    }

    /** */
    public void register(TestMessageService service) {
        srvcMap.put(service.localNodeId, service);
        service.mgr = this;
    }
}
