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
import io.socket.client.Socket;
import io.socket.emitter.Emitter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.console.agent.AgentUtils.safeCallback;
import static org.apache.ignite.console.agent.AgentUtils.toJSON;

/**
 *
 */
public class DemoListener {
    /** */
    private static final String EVENT_DEMO_TOPOLOGY = "demo:topology";

    /** Default timeout. */
    private static final long DFLT_TIMEOUT = 3000L;

    /** */
    private static final Logger log = LoggerFactory.getLogger(DemoListener.class);

    /** */
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(2);

    /** */
    private ScheduledFuture<?> refreshTask;

    /** */
    private Socket client;

    /** */
    private RestExecutor restExecutor;

    /**
     * @param client Client.
     * @param restExecutor Client.
     */
    public DemoListener(Socket client, RestExecutor restExecutor) {
        this.client = client;
        this.restExecutor = restExecutor;
    }

    /**
     * Start broadcast topology to server-side.
     */
    public Emitter.Listener start() {
        return new Emitter.Listener() {
            @Override public void call(final Object... args) {
                final Ack demoStartCb = safeCallback(args);

                final long timeout = args.length > 1  && args[1] instanceof Long ? (long)args[1] : DFLT_TIMEOUT;

                if (refreshTask != null)
                    refreshTask.cancel(true);

                final CountDownLatch latch = AgentClusterDemo.tryStart();

                pool.schedule(new Runnable() {
                    @Override public void run() {
                        try {
                            U.await(latch);

                            demoStartCb.call();

                            refreshTask = pool.scheduleWithFixedDelay(new Runnable() {
                                @Override public void run() {
                                    try {
                                        RestResult top = restExecutor.topology(true, true);

                                        client.emit(EVENT_DEMO_TOPOLOGY, toJSON(top));
                                    }
                                    catch (IOException e) {
                                        log.info("Lost connection to the demo cluster", e);

                                        stop().call(); // TODO WTF????
                                    }
                                }
                            }, 0L, timeout, TimeUnit.MILLISECONDS);
                        }
                        catch (Exception e) {
                            demoStartCb.call(e);
                        }
                    }
                }, 0, TimeUnit.MILLISECONDS);
            }
        };
    }

    /**
     * Stop broadcast topology to server-side.
     */
    public Emitter.Listener stop() {
        return new Emitter.Listener() {
            @Override public void call(Object... args) {
                refreshTask.cancel(true);

                AgentClusterDemo.stop();
            }
        };
    }
}
