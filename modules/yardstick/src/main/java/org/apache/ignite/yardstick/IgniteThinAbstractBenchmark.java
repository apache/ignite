/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.yardstick;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.yardstick.thin.cache.IgniteThinBenchmarkUtils;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriverAdapter;
import org.yardstickframework.BenchmarkUtils;

import static org.yardstickframework.BenchmarkUtils.jcommander;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Abstract class for Thin client benchmarks.
 */
public abstract class IgniteThinAbstractBenchmark extends BenchmarkDriverAdapter {
    /** Arguments. */
    protected final IgniteBenchmarkArguments args = new IgniteBenchmarkArguments();

    /** Client. */
    private ThreadLocal<IgniteClient> client;

    /** Server host addresses queue. */
    private ConcurrentLinkedDeque<String> servHosts;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        jcommander(cfg.commandLineArguments(), args, "<ignite-driver>");

        String locIp = IgniteThinBenchmarkUtils.getLocalIp(cfg);

        client = new ThreadLocal<IgniteClient>() {
            @Override protected IgniteClient initialValue() {
                synchronized (IgniteThinAbstractBenchmark.class) {
                    try {
                        if (servHosts == null || servHosts.isEmpty())
                            setServHosts(cfg);

                        return new IgniteThinClient().start(cfg, servHosts.poll());
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    return null;
                }
            }
        };

        println("Custom properties:");

        for (String prop : cfg.customProperties().keySet())
            println(String.format("%s=%s", prop, cfg.customProperties().get(prop)));

        // Create util cache for checking if all driver processes had been started.
        ClientCache<String, String> utilCache = client().getOrCreateCache("start-util-cache");

        // Put 'started' message in util cache.
        utilCache.put(locIp, "started");

        List<String> hostList = IgniteThinBenchmarkUtils.drvHostList(cfg);

        int cnt = 0;

        // Wait for all driver processes to start.
        while(!checkIfAllClientsStarted(hostList) && cnt++ < 600)
            Thread.sleep(500L);
    }

    /**
     *
     * @param cfg
     */
    private synchronized void setServHosts(BenchmarkConfiguration cfg){
        BenchmarkUtils.println("Setting serv host queue");

        String[] servHostArr = IgniteThinBenchmarkUtils.servHostArr(cfg);

        servHosts = new ConcurrentLinkedDeque<>(Arrays.asList(servHostArr));
    }

    /**
     * Check if all driver processes had been started.
     *
     * @param hostList List of driver host addresses.
     * @return {@code true} if all driver processes had been started or {@code false} if not.
     */
    private boolean checkIfAllClientsStarted(List<String> hostList){
        ClientCache<String, String> utilCache = client().getOrCreateCache("start-util-cache");

        for(String host : hostList){
            if(host.equals("localhost"))
                host = "127.0.0.1";

            String res = utilCache.get(host);

            if (res == null || !res.equals("started"))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (client.get() != null)
            client.get().close();
    }

    /** {@inheritDoc} */
    @Override public String description() {
        String desc = BenchmarkUtils.description(cfg, this);

        return desc.isEmpty() ?
            getClass().getSimpleName() + args.description() + cfg.defaultDescription() : desc;
    }

    /**
     * @return Client.
     */
    protected IgniteClient client() {
        return client.get();
    }

    /**
     * @param max Key range.
     * @return Next key.
     */
    public static int nextRandom(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }

    /**
     * @param min Minimum key in range.
     * @param max Maximum key in range.
     * @return Next key.
     */
    protected int nextRandom(int min, int max) {
        return ThreadLocalRandom.current().nextInt(max - min) + min;
    }
}
