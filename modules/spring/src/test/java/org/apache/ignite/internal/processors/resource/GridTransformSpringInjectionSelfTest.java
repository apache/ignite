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

package org.apache.ignite.internal.processors.resource;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class GridTransformSpringInjectionSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        IgniteSpring.start(getConfiguration(getTestIgniteInstanceName(0)),
            new ClassPathXmlApplicationContext("/org/apache/ignite/internal/processors/resource/spring-resource.xml"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransformResourceInjection() throws Exception {
        Ignite grid = grid(0);

        IgniteCache<String, Integer> cache = grid.createCache(cacheConfiguration(ATOMIC));

        try {
            doTransformResourceInjection(cache);
        }
        finally {
            cache.destroy();
        }

        cache = grid.createCache(cacheConfiguration(TRANSACTIONAL));

        try {
            doTransformResourceInjection(cache);

            for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    IgniteTransactions txs = grid.transactions();

                    try (Transaction tx = txs.txStart(concurrency, isolation)) {
                        doTransformResourceInjection(cache);

                        tx.commit();
                    }
                }
            }
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<String, Integer> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration<String, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setName(getClass().getSimpleName());
        ccfg.setAtomicityMode(atomicityMode);

        return ccfg;
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void doTransformResourceInjection(IgniteCache<String, Integer> cache) throws Exception {
        final Collection<ResourceType> required = Arrays.asList(
            ResourceType.SPRING_APPLICATION_CONTEXT,
            ResourceType.SPRING_BEAN);

        Integer flags = cache.invoke(UUID.randomUUID().toString(), new SpringResourceInjectionEntryProcessor());

        assertTrue("Processor result is null", flags != null);

        log.info("Injection flag: " + Integer.toBinaryString(flags));

        Collection<ResourceType> notInjected = ResourceInfoSet.valueOf(flags).notInjected(required);

        if (!notInjected.isEmpty())
            fail("Can't inject resource(s): " + Arrays.toString(notInjected.toArray()));

        Set<String> keys = new HashSet<>(Arrays.asList(UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()));

        Map<String, EntryProcessorResult<Integer>> results = cache.invokeAll(keys,
            new SpringResourceInjectionEntryProcessor());

        assertEquals(keys.size(), results.size());

        for (EntryProcessorResult<Integer> res : results.values()) {
            Collection<ResourceType> notInjected1 = ResourceInfoSet.valueOf(res.get()).notInjected(required);

            if (!notInjected1.isEmpty())
                fail("Can't inject resource(s): " + Arrays.toString(notInjected1.toArray()));
        }
    }

    /**
     *
     */
    static class SpringResourceInjectionEntryProcessor extends ResourceInjectionEntryProcessorBase<String, Integer> {
        /** */
        private transient ApplicationContext appCtx;

        /** */
        private transient GridSpringResourceInjectionSelfTest.DummyResourceBean dummyBean;

        /**
         * @param appCtx Context.
         */
        @SpringApplicationContextResource
        public void setApplicationContext(ApplicationContext appCtx) {
            assert appCtx != null;

            checkSet();

            infoSet.set(ResourceType.SPRING_APPLICATION_CONTEXT, true);

            this.appCtx = appCtx;
        }

        /**
         * @param dummyBean Resource bean.
         */
        @SpringResource(resourceName = "dummyResourceBean")
        public void setDummyBean(GridSpringResourceInjectionSelfTest.DummyResourceBean dummyBean) {
            assert dummyBean != null;

            checkSet();

            infoSet.set(ResourceType.SPRING_BEAN, true);

            this.dummyBean = dummyBean;
        }
    }
}
