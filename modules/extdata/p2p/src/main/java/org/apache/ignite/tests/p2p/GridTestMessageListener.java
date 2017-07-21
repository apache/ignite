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

package org.apache.ignite.tests.p2p;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Test message listener.
 */
public class GridTestMessageListener implements P2<UUID, Object> {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean apply(UUID nodeId, Object msg) {
        ignite.log().info("Received message [nodeId=" + nodeId + ", locNodeId=" + ignite.cluster().localNode().id() +
            ", msg=" + msg + ']');

        ConcurrentMap<String, AtomicInteger> map = ignite.cluster().nodeLocalMap();

        AtomicInteger cnt = map.get("msgCnt");

        if (cnt == null) {
            AtomicInteger old = map.putIfAbsent("msgCnt", cnt = new AtomicInteger(0));

            if (old != null)
                cnt = old;
        }

        cnt.incrementAndGet();

        return true;
    }
}