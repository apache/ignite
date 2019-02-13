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

package org.apache.ignite.internal.processor.security;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;

/**
 *
 */
public abstract class AbstractCacheOperationRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(getCacheConfigurations());
    }

    /**
     * Getting array of cache configurations.
     */
    protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
        };
    }

    /**
     * Sets up VERIFIER, performs the runnable and checks the result.
     *
     * @param node Node.
     * @param r Runnable.
     */
    protected final void runAndCheck(IgniteEx node, Runnable r){
        VERIFIER.start(secSubjectId(node))
            .add(SRV_TRANSITION, 1)
            .add(SRV_ENDPOINT, 1);

        r.run();

        VERIFIER.checkResult();
    }

    /**
     * Getting the key that is contained on primary partition on passed node for {@link #CACHE_NAME} cache.
     *
     * @param ignite Node.
     * @return Key.
     */
    protected static Integer prmKey(IgniteEx ignite) {
        Affinity<Integer> affinity = ignite.affinity(CACHE_NAME);

        int i = 0;
        do {
            if (affinity.isPrimary(ignite.localNode(), ++i))
                return i;

        }
        while (i <= 1_000);

        throw new IllegalStateException(ignite.name() + " isn't primary node for any key.");
    }

    /**
     * Getting the key that is contained on primary partition on passed node for {@link #CACHE_NAME} cache.
     *
     * @param nodeName Node name.
     * @return Key.
     */
    protected static Integer prmKey(String nodeName) {
        return prmKey((IgniteEx)G.ignite(nodeName));
    }
}
