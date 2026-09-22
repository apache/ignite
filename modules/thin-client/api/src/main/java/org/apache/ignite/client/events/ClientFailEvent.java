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

import org.apache.ignite.configuration.ClientConfiguration;

/**
 * Event, triggered when Ignite client is failed to start.
 */
public class ClientFailEvent implements ClientLifecycleEvent {
    /** */
    private final ClientConfiguration cfg;

    /** */
    private final Throwable throwable;

    /**
     * @param cfg Client configuration.
     * @param throwable Throwable that caused the failure.
     */
    public ClientFailEvent(ClientConfiguration cfg, Throwable throwable) {
        this.cfg = cfg;
        this.throwable = throwable;
    }

    /**
     * @return Client configuration.
     */
    public ClientConfiguration configuration() {
        return cfg;
    }

    /**
     * @return A cause of the failure.
     */
    public Throwable throwable() {
        return throwable;
    }
}
