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

package org.apache.ignite.yardstick.cache.failover;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteNode;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkConfiguration.InstanceType.SERVER;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * TODO: Add class description.
 */
public class IgniteFailoverNode extends IgniteNode {
    @Override public void start(BenchmarkConfiguration cfg) throws Exception {
        super.start(cfg);

        println(">>>>> at IgniteFailoverNode");

        if (cfg.instanceType() == SERVER) {
            RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();

            List<String> jvmOpts = mxBean.getInputArguments();

            println(">>>>>> jvmOpts=" + jvmOpts);
            println(">>>>>> cp=" + mxBean.getClassPath());

            println(">>>>>> CUR_DIR=" + System.getenv("CUR_DIR"));
            println(">>>>>> SCRIPT_DIR=" + System.getenv("SCRIPT_DIR"));
            println(">>>>>> LOGS_DIR=" + System.getenv("LOGS_DIR"));

            IgniteCache<Integer, String[]> srvsCfgsCache = ignite().
                getOrCreateCache(new CacheConfiguration<Integer, String[]>().setName("serversConfigs"));

            println(">>>>> env = " + System.getenv().toString());

            srvsCfgsCache.put(cfg.memberId(), cfg.commandLineArguments());

            println("Put at cache " + cfg.memberId() + "=" + Arrays.toString(cfg.commandLineArguments()));
        }
        else
            println(">>>>>> Oooooaaaaiiiii CLient node here!!!");
    }
}
