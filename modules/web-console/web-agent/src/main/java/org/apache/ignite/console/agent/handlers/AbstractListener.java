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

package org.apache.ignite.console.agent.handlers;

import io.socket.client.Ack;
import io.socket.emitter.Emitter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;

import static org.apache.ignite.console.agent.AgentUtils.removeCallback;
import static org.apache.ignite.console.agent.AgentUtils.fromJSON;
import static org.apache.ignite.console.agent.AgentUtils.safeCallback;
import static org.apache.ignite.console.agent.AgentUtils.toJSON;

/**
 * Base class for web socket handlers.
 */
abstract class AbstractListener implements Emitter.Listener {
    /** */
    private ExecutorService pool;

    /** */
    final Logger log = Logger.getLogger(this.getClass().getName());

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final void call(Object... args) {
        final Ack cb = safeCallback(args);

        args = removeCallback(args);

        try {
            final Map<String, Object> params;

            if (args == null || args.length == 0)
                params = Collections.emptyMap();
            else if (args.length == 1)
                params = fromJSON(args[0], Map.class);
            else
                throw new IllegalArgumentException("Wrong arguments count, must be <= 1: " + Arrays.toString(args));

            if (pool == null)
                pool = newThreadPool();

            pool.submit(new Runnable() {
                @Override public void run() {
                    try {
                        Object res = execute(params);

                        cb.call(null, toJSON(res));
                    } catch (Exception e) {
                        cb.call(e, null);
                    }
                }
            });
        }
        catch (Exception e) {
            cb.call(e, null);
        }
    }

    /**
     * Stop handler.
     */
    public void stop() {
        if (pool != null)
            pool.shutdownNow();
    }

    /**
     * Creates a thread pool that can schedule commands to run after a given delay, or to execute periodically.
     *
     * @return Newly created thread pool.
     */
    protected ExecutorService newThreadPool() {
        return Executors.newSingleThreadExecutor();
    }

    /**
     * Execute command with specified arguments.
     *
     * @param args Map with method args.
     */
    public abstract Object execute(Map<String, Object> args) throws Exception;
}
