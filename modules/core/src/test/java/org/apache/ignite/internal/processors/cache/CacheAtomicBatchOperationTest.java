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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.jsr166.ConcurrentHashMap8;

/**
 * Bath operation with atomic cache test
 */
public class CacheAtomicBatchOperationTest extends GridCommonAbstractTest {

    private static final int BULK_SIZE = 10_000;
    private static final int STEP_COUNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        cfg.setMarshaller(new BinaryMarshaller());

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(true);

        bCfg.setIdMapper(new BinaryBasicIdMapper(false));
        bCfg.setNameMapper(new BinaryBasicNameMapper(false));

        cfg.setBinaryConfiguration(bCfg);

        return cfg;
    }

    public void testAtomicBatchOperation() throws Exception {

        CacheConfiguration<String, String> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        try (Ignite ignite1 = startGrid(1); Ignite ignite2 = startGrid(2)) {
            Object objectTestMap[] = generateObjectMaps();
            Object binaryTestMap[] = generateBinaryMaps(ignite1);
            for (int i = 0; i < STEP_COUNT; i++) {
                doTestObjectMaps(ignite1, ignite2, cacheCfg, objectTestMap);
                doTestBinaryMaps(ignite1, ignite2, cacheCfg, binaryTestMap);
            }

        }
        finally {
            stopAllGrids();
        }
    }

    private static void doTestObjectMaps(Ignite ignite1, Ignite ignite2, CacheConfiguration cacheCfg,
        Object objectMaps[]) {
        Map<String, String> hashMap = (Map<String, String>)objectMaps[0];
        Map<String, String> treeMap = (Map<String, String>)objectMaps[1];
        // Insert same data from two nodes in batch mode
        IgniteCompute compute11 = ignite1.compute().withAsync();
        compute11.run(() -> {
            IgniteCache cache = ignite1.getOrCreateCache(cacheCfg);
            cache.putAll(treeMap);
        });

        IgniteCompute compute12 = ignite2.compute().withAsync();
        compute12.run(() -> {
            IgniteCache cache = ignite2.getOrCreateCache(cacheCfg);
            cache.putAll(hashMap);
        });

        compute11.future().get();
        compute12.future().get();

        // Remove elements
        IgniteCache cache = ignite1.getOrCreateCache(cacheCfg);
        cache.removeAll(treeMap.keySet());
    }

    private static void doTestBinaryMaps(Ignite ignite1, Ignite ignite2, CacheConfiguration cacheCfg,
        Object objectMaps[]) throws Exception {
        Map<BinaryObject, String> hashMap = (Map<BinaryObject, String>)objectMaps[0];
        Map<BinaryObject, String> treeMap = (Map<BinaryObject, String>)objectMaps[1];
        // Insert same data from two nodes in batch mode
        Map.Entry<BinaryObject, String> entry = treeMap.entrySet().iterator().next();
        IgniteCache cache0 = ignite1.getOrCreateCache(cacheCfg);
        Thread t1 = new Thread(() -> {
            IgniteCache cache = ignite1.getOrCreateCache(cacheCfg);
            cache.putAll(treeMap);
        });
        Thread t2 = new Thread(() -> {
            IgniteCache cache = ignite2.getOrCreateCache(cacheCfg);

            cache.putAll(hashMap);
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();

        // Remove elements
        IgniteCache cache = ignite1.getOrCreateCache(cacheCfg);
        cache.removeAll(treeMap.keySet());
    }

    private static Object[] generateObjectMaps() {
        Map<String, String> hashMap = new HashMap<>();
        for (int i = 0; i < BULK_SIZE; i++) {
            hashMap.put(String.valueOf(i), String.valueOf(i));
        }
        Map<String, String> treeMap = new TreeMap<>(hashMap);
        return new Object[] {hashMap, treeMap};
    }

    private static Object[] generateBinaryMaps(Ignite ignite1) {
        Map<BinaryObject, String> hashMap = new HashMap<>();

        int blockSize = BULK_SIZE / 3;
        int l = 0;

        for (int i = 0; i < blockSize; i++) {

            String val = String.valueOf(l);

            BinaryObject key = ignite1.binary().toBinary(new TestStaticClass(val));//builder.build();
            hashMap.put(key, val);
            l++;
        }

        IgniteBinary binary = ignite1.binary();
        BinaryObjectBuilder builder = binary.builder("TestClass");
        for (int i = 0; i < blockSize; i++) {
            String val = String.valueOf(l);
            builder.setField("testField", val);
            builder.hashCode(l);
            BinaryObject key = builder.build();

            hashMap.put(key, val);
            l++;
        }

        for (int i = l; i < blockSize / 2; i++) {
            String val = String.valueOf(l);
            BinaryObject key = binary.buildEnum("TestEnum", l);
            hashMap.put(key, val);
            hashMap.put(key, val);
            l++;
        }

        for (int i = l; i < BULK_SIZE; i++) {
            String val = String.valueOf(i);
            BinaryObject key = binary.buildEnum("TestEnum2", i);
            hashMap.put(key, val);
            hashMap.put(key, val);
        }

        ConcurrentHashMap8<BinaryObject, String> anotherMap = new ConcurrentHashMap8(hashMap);

        return new Object[] {hashMap, anotherMap};
    }

    private static class TestStaticClass {
        private final String testField;

        public TestStaticClass(String testField) {
            this.testField = testField;
        }

        @Override public int hashCode() {
            return testField.hashCode();
        }

        @Override public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (!(obj instanceof  TestStaticClass))
                return false;
            TestStaticClass other = (TestStaticClass)obj;
            return (this.testField == null && other.testField==null)
                || (this.testField != null && this.testField.equals(other.testField));
        }

        public String getTestField() {
            return testField;
        }
    }
}