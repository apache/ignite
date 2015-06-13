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

package org.apache.ignite.spring.autowired;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.springframework.beans.factory.*;

import javax.cache.configuration.*;

/**
 * Test service.
 */
public class SpringAutowiredTestService implements InitializingBean {
    /** */
    public static CacheAtomicityMode mode;

    /** */
    private Ignite ignite;

    /** */
    private IgniteCache<Integer, Integer> cache;

    /**
     * @param ignite Ignite.
     */
    public void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public void afterPropertiesSet() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(SpringAutowiredSelfTest.CACHE_NAME);
        ccfg.setAtomicityMode(mode);
        ccfg.setCacheStoreFactory(FactoryBuilder.factoryOf(SpringAutowiredTestStore.class));
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);

        cache = ignite.createCache(ccfg);
    }

    /**
     */
    public void run() {
        for (int i = 0; i < 100; i++)
            cache.get(i);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        for (int i = 0; i < 100; i++)
            cache.remove(i);
    }
}
