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

package org.apache.ignite.prometheus;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 *
 */
public class Collector implements LifecycleBean {

    private HTTPServer server;
    private Integer port;
    private Integer portRange;

    @IgniteInstanceResource
    private Ignite ignite;
    private IgniteLogger log;

    private static final Integer DEFAULT_PORT = 1234;
    private static final Integer DEFAULT_PORT_RANGE = 100;

    public Collector() {
        this(DEFAULT_PORT, DEFAULT_PORT_RANGE);
    }

    public Collector(Integer port) {
        this(port, DEFAULT_PORT_RANGE);
    }

    public Collector(Integer port, Integer portRange) {
        this.port = port;
        this.portRange = portRange;
    }

    @Override
    public void onLifecycleEvent(LifecycleEventType evt) {
        if (log == null) {
            log = ignite.log();
        }
        if (evt == LifecycleEventType.AFTER_NODE_START) {
            // Start JVM stats
            DefaultExports.initialize();

            // Start our custm stats
            new IgniteMetricCollector(ignite).register();

            // Let's get this show on the road
            Integer maxPort = port + portRange;
            while (server == null && port < maxPort) {
                try {
                    server = new HTTPServer(port, true);

                    log.info("Started Prometheus server on port " + port);
                } catch (java.io.IOException e) {
                    port++;
                }
            }
            if (server == null) {
                log.info("Couldn't start Prometheus server");
            }
        }
        else if (evt == LifecycleEventType.BEFORE_NODE_STOP) {
            if (server != null) {
                server.stop();
            }
            log.info("Stopping Prometheus server");
        }
    }
}
