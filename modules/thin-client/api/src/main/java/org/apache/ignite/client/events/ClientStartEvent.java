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

package org.apache.ignite.client.events;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

/**
 * Event, triggered when Ignite client is started.
 */
public class ClientStartEvent implements ClientLifecycleEvent {
    /** */
    private final IgniteClient client;

    /** */
    private final ClientConfiguration cfg;

    /**
     * @param client Ignite client instance.
     * @param cfg Client configuration.
     */
    public ClientStartEvent(IgniteClient client, ClientConfiguration cfg) {
        this.client = client;
        this.cfg = cfg;
    }

    /**
     * @return Client configuration.
     */
    public ClientConfiguration configuration() {
        return cfg;
    }

    /**
     * @return Ignite client instance.
     */
    public IgniteClient client() {
        return client;
    }
}
