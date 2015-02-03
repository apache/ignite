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

package org.apache.ignite.loadtests.streamer;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Streamer benchmark.
 */
public class GridStreamerBenchmark {

    /**
     * Entry point. Expects configuration URL to be provided.
     *
     * @param args Arguments. First argument is grid configuration. Second optional argument "-w" - stands for
     *    "worker", in this case no load will be generated on the node.
     * @throws Exception In case of any error.
     */
    public static void main(String[] args) throws Exception{
        if (args.length == 0)
            throw new IllegalArgumentException("Configuration path is not provided.");

        String cfgPath = args.length > 0 ? args[0] :
            "modules/core/src/test/config/streamer/average/spring-streamer-average-local.xml";

        boolean worker = args.length > 1 && "-w".equalsIgnoreCase(args[1]);

        // Get load definitions.
        Collection<GridStreamerLoad> loads = worker ? null : loads(cfgPath);

        // Start the grid.
        Ignite ignite = G.start(cfgPath);

        // Start load threads.
        Collection<Thread> loadThreads = new HashSet<>();

        if (loads != null && !loads.isEmpty()) {
            for (GridStreamerLoad load : loads) {
                final IgniteStreamer streamer = ignite.streamer(load.getName());

                if (streamer == null)
                    throw new Exception("Steamer is not found: " + load.getName());

                List<IgniteInClosure<IgniteStreamer>> clos = load.getClosures();

                if (clos != null && !clos.isEmpty()) {
                    for (final IgniteInClosure<IgniteStreamer> clo : clos) {
                        Thread t = new Thread(new Runnable() {
                            @Override public void run() {
                                try {
                                    clo.apply(streamer);
                                }
                                catch (Exception e) {
                                    X.println("Exception during execution of closure for streamer " +
                                        "[streamer=" + streamer.name() + ", closure=" + clo + ", err=" +
                                        e.getMessage() + ']');

                                    e.printStackTrace();
                                }
                            }
                        });

                        loadThreads.add(t);

                        t.start();
                    }
                }
            }
        }

        // Once all loads are started, simply join them.
        System.out.println("Press enter to stop running benchmark.");

        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
            in.readLine();
        }

        for (Thread t : loadThreads)
            t.interrupt();

        for (Thread t : loadThreads)
            t.join();
    }

    /**
     * Get loads from the Spring context.
     *
     * @param cfgPath Configuration path.
     * @return Collection of loads, if any.
     * @throws Exception If failed.
     */
    private static Collection<GridStreamerLoad> loads(String cfgPath) throws Exception {
        URL cfgUrl;

        try {
            cfgUrl = new URL(cfgPath);
        }
        catch (MalformedURLException ignore) {
            cfgUrl = U.resolveGridGainUrl(cfgPath);
        }

        if (cfgUrl == null)
            throw new Exception("Spring XML configuration path is invalid: " + cfgPath);

        GenericApplicationContext springCtx = new GenericApplicationContext();

        new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(cfgUrl));

        springCtx.refresh();

        Map<String, GridStreamerLoad> cfgMap = springCtx.getBeansOfType(GridStreamerLoad.class);

        return cfgMap.values();
    }
}
