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

package org.apache.ignite.internal.processors.ssl;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

/** SSL factories used by running transports. */
public class GridSslProcessor extends GridProcessorAdapter {
    /** */
    private final Map<String, Factory<SSLContext>> factories = new LinkedHashMap<>();

    /** @param ctx Kernal context. */
    public GridSslProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param transport Transport name.
     * @param factory SSL context factory.
     */
    public synchronized void register(String transport, Factory<SSLContext> factory) {
        factories.put(transport, factory);
    }

    /** @return Snapshot of active transport factories. */
    public synchronized Map<String, Factory<SSLContext>> factories() {
        return new LinkedHashMap<>(factories);
    }
}
