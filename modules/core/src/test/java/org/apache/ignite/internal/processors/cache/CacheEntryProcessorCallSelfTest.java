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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for value copy in entry processor.
 */
public class CacheEntryProcessorCallSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEpCall() throws Exception {

        Ignite ignite = startGrids(2);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCopyOnRead(true);
        //ccfg.setNearConfiguration(null);

        IgniteCache<Integer, String> cache = null;

        try {
            cache = ignite.createCache(ccfg);

            ArrayList<Integer> primaryKeys = new ArrayList<>();

            primaryKeys.add(generateNodeKeys(grid(0), cache));
            primaryKeys.add(generateNodeKeys(grid(1), cache));

            log.info("Thread=" + Thread.currentThread().toString());
            log.info("Local node: " + ignite.cluster().localNode().id());

            try{
                //Ignite ignite1 = Ignition.ignite();
                //Ignite ignite2 = Ignition.localIgnite();
                //MutableEntry.unwrap(Ignite.class);
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }

            for(Integer key : primaryKeys) {
                cache.put(key, "=" + key);

                final String valueNew = "Inside invoke";

                String prev = cache.invoke(key, new CacheEntryProcessor<Integer, String, String>() {
                    @IgniteInstanceResource
                    private Ignite ignite;

                    @Override public String process(MutableEntry<Integer, String> entry, Object... args) {
                        String val = entry.getValue();

                        try{
                            Ignite ignite2 = Ignition.localIgnite();
                            Ignite ignite1 = Ignition.ignite();
                        }
                        catch(Exception ex) {
                            ex.printStackTrace();
                        }

                        ignite = Ignition.localIgnite();

                        entry.setValue(valueNew);

                        log.info("Thread=" + Thread.currentThread().toString());
                        log.info("Local node: " + ignite.cluster().localNode().id());

                        return val;
                    }
                });

                String valueLast = cache.get(key);;

                assertEquals(valueLast, valueNew);
            }
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @param node Node.
     * @param cache Cache.
     * @return Key for cache which primary for given node.
     */
    protected Integer generateNodeKeys(IgniteEx node, IgniteCache cache) {
        ClusterNode locNode = node.localNode();

        Affinity<Integer> aff = (Affinity<Integer>)affinity(cache);

        for (Integer ind = 0; ind < 100_000; ind++) {
            if (aff.isPrimary(locNode, ind))
                return ind;
        }

        throw new IgniteException("Unable to find keys as primary for cache: " + node);
    }

}
