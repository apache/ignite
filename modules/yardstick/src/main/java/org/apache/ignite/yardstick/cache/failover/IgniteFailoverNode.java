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
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteNode;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite failover node.
 */
public class IgniteFailoverNode extends IgniteNode {
    /** {@inheritDoc} */
    @Override public void start(BenchmarkConfiguration cfg) throws Exception {
        super.start(cfg);

        // Put server configuration at special cache.
        RuntimeMXBean mxBean = ManagementFactory.getRuntimeMXBean();

        List<String> jvmOpts = mxBean.getInputArguments();

        StringBuilder jvmOptsStr = new StringBuilder();

        for (String opt : jvmOpts)
            jvmOptsStr.append(opt).append(' ');

        cfg.customProperties().put("JVM_OPTS", jvmOptsStr.toString());
        cfg.customProperties().put("PROPS_ENV", System.getenv("PROPS_ENV"));
        cfg.customProperties().put("CLASSPATH", mxBean.getClassPath());
        cfg.customProperties().put("JAVA", System.getenv("JAVA"));

        IgniteCache<Integer, BenchmarkConfiguration> srvsCfgsCache = ignite().
            getOrCreateCache(new CacheConfiguration<Integer, BenchmarkConfiguration>().setName("serversConfigs"));

        srvsCfgsCache.put(cfg.memberId(), cfg);

        println("Put at cache [" + cfg.memberId() + "=" + cfg + "]");
    }
}
