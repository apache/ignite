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

package org.apache.ignite.yardstick.cache.load;

import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.apache.ignite.yardstick.cache.load.model.HeavyValue;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs put operations.
 */
public class IgniteWALModesLoadBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        load(cache());

        return true; // Cause benchmark stop.
    }

    /**
     * @param c Closure.
     */
    private void load(IgniteCache c) {
        if (c.size() != 0)
            throw new RuntimeException("Cache is not empty!");

        System.out.println("Loading ... " +
            ignite().configuration().getPersistentStoreConfiguration().getWalMode() + " " + new Date().toString());

        long start = System.currentTimeMillis();

        IgniteDataStreamer<Integer, HeavyValue> dataLdr = ignite().dataStreamer(c.getName());

        for (int i = 0; i < args.range(); i++) {
            if (i % 100_000 == 0)
                System.out.println("... " + i);

            dataLdr.addData(i, HeavyValue.generate());
        }

        dataLdr.close();

        dataLdr.future().get();

        System.out.println("Loaded ... " + new Date().toString());

        long time = System.currentTimeMillis() - start;

        BenchmarkUtils.println("IgniteStreamerBenchmark finished load cache [totalSeconds=" + time / 1000 + ']');

        if (c.size() != args.preloadAmount())
            throw new RuntimeException("Loading failed. actual size =" + c.size());
    }

    /**
     * @param indexed Indexed.
     */
    CacheConfiguration<Integer, Object> cacheCfg(boolean indexed) {
        CacheConfiguration cacheCfg = new CacheConfiguration<Object, HeavyValue>("wal-cache");

        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setWriteSynchronizationMode(args.syncMode());
        cacheCfg.setBackups(args.backups());
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        if (indexed) {
            QueryEntity entity = new QueryEntity(Integer.class.getName(), HeavyValue.class.getName());

            LinkedHashMap<String, String> fields = new LinkedHashMap<>();

            for (int i = 1; i < 50; i++)
                fields.put("field" + i, i < 44 ? "java.lang.String" : "java.lang.Double");

            entity.setFields(fields);

            QueryIndex idx1 = new QueryIndex("field1");
            QueryIndex idx2 = new QueryIndex("field2");
            QueryIndex idx3 = new QueryIndex("field3");
            QueryIndex idx4 = new QueryIndex("field4");
            QueryIndex idx5 = new QueryIndex("field5");

            entity.setIndexes(Arrays.asList(
                idx1,
                idx2,
                idx3,
                idx4,
                idx5
            ));

            cacheCfg.setQueryEntities(Arrays.asList(entity));
        }

        return cacheCfg;
    }

    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().getOrCreateCache(cacheCfg(false));
    }
}
