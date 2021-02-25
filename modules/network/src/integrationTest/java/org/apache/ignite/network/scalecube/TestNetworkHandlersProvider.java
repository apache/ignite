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


package org.apache.ignite.network.scalecube;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.network.NetworkClusterEventHandler;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;

/** */
class TestNetworkHandlersProvider implements NetworkHandlersProvider {
    /** */
    public static Map<String, NetworkMessage> MESSAGE_STORAGE = new ConcurrentHashMap<>();

    /** */
    private final String localName;

    /** */
    TestNetworkHandlersProvider(String name) {
        localName = name;
    }

    /** {@inheritDoc} */
    @Override public NetworkMessageHandler messageHandler() {
        return event -> {
            MESSAGE_STORAGE.put(localName, event);

            System.out.println(localName + " handled messages : " + event);
        };
    }

    /** {@inheritDoc} */
    @Override public NetworkClusterEventHandler clusterEventHandler() {
        return new NetworkClusterEventHandler() {
            @Override public void onAppeared(NetworkMember member) {
                System.out.println(localName + " found member : " + member);
            }

            @Override public void onDisappeared(NetworkMember member) {
                System.out.println(localName + " lost member : " + member);
            }
        };
    }
}
