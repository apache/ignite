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
import java.util.HashMap;
import java.util.Map;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class BinaryMetadataRegistrationInsideEntryProcessorTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder()
            .setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        return new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder))
            .setPeerClassLoadingEnabled(true);
    }

    /**
     * @throws Exception If failed;
     */
    public void test() throws Exception {
        Ignite ignite = startGrids(2);

        IgniteCache<Integer, Map<Integer, CustomObj>> cache = ignite.createCache(CACHE_NAME);

        try {
            for (int i = 0; i < 10_000; i++)
                cache.invoke(i, new CustomProcessor());
        }
        catch (Exception e) {
            Map<Integer, CustomObj> value = cache.get(1);

            if ((value != null) && (value.get(1) != null) && (value.get(1).getObj() == CustomEnum.ONE))
                System.out.println("Data was saved.");
            else
                System.out.println("Data wasn't saved.");

            throw e;
        }
    }

    /**
     *
     */
    private static class CustomProcessor implements EntryProcessor<Integer,
        Map<Integer, CustomObj>, Object> {
        /** {@inheritDoc} */
        @Override public Object process(
            MutableEntry<Integer, Map<Integer, CustomObj>> entry,
            Object... objects) throws EntryProcessorException {
            Map<Integer, CustomObj> map = new HashMap<>();

            map.put(1, new CustomObj(CustomEnum.ONE));

            entry.setValue(map);

            return null;
        }
    }

    /**
     *
     */
    private static class CustomObj {
        /** Object. */
        private final Object obj;

        /**
         * @param obj Object.
         */
        public CustomObj(Object obj) {
            this.obj = obj;
        }

        /**
         * @param val Value.
         */
        public static CustomObj valueOf(int val) {
            return new CustomObj(val);
        }

        /**
         *
         */
        public Object getObj() {
            return obj;
        }
    }

    /**
     *
     */
    private enum CustomEnum {
        /** */ONE(1),
        /** */TWO(2),
        /** */THREE(3);

        /** Value. */
        private final Object val;

        /**
         * @param val Value.
         */
        CustomEnum(Object val) {
            this.val = val;
        }
    }

}