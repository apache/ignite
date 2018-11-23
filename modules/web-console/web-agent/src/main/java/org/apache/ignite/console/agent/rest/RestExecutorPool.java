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

package org.apache.ignite.console.agent.rest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.Nullable;

/**
 * Class that store collection of {@link RestExecutor}.
 */
public class RestExecutorPool implements AutoCloseable {
    /** */
    private final Map<String, RestExecutor> pool = new ConcurrentHashMap<>();

    /**
     * Get REST executor by name.
     *
     * @param name REST executor name.
     * @return Existing REST executor or {@code null} if not found.
     */
    @Nullable public RestExecutor get(String name) {
        return pool.get(name);
    }

    /**
     * Create new REST executor and cache it.
     *
     * @param name REST executor name.
     * @param clientStore Optional path to client key store file.
     * @param clientPwd Optional password for client key store.
     * @param trustStore Optional path to trust key store file.
     * @param trustStorePwd Optional password for trust key store.
     * @return New or existing REST executor.
     */
    public RestExecutor open(
        String name,
        String clientStore,
        String clientPwd,
        String trustStore,
        String trustStorePwd
    ) throws Exception {
        RestExecutor exec = pool.get(name);

        if (exec == null)
            exec = pool.putIfAbsent(name, new RestExecutor(clientStore, clientPwd, trustStore, trustStorePwd));

        return exec;
    }

    /**
     * Close executor for specified name.
     *
     * @param name Executor name.
     */
    public void close(String name) {
        RestExecutor restExec = pool.remove(name);

        if (restExec != null)
            restExec.close();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        pool.values().forEach(RestExecutor::close);
        pool.clear();
    }
}
