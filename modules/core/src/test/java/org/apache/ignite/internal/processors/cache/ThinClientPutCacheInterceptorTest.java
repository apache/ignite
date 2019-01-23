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

import javax.cache.Cache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ThinClientPutCacheInterceptorTest extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration("test").setInterceptor(new ThinBinaryValueInterceptor()))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration())
            .setFailureHandler(new StopNodeFailureHandler());
    }

    @Test
    public void test() throws Exception {
        try (IgniteEx ignite = startGrid(0)) {

            ignite.cluster().active(true);

            ClientConfiguration ccfg = new ClientConfiguration()
                .setAddresses("127.0.0.1:" + ignite.configuration().getClientConnectorConfiguration().getPort());

            try (IgniteClient igniteClient = Ignition.startClient(ccfg)) {
                ClientCache<String, ThinBinaryValue> cache = igniteClient.cache("test");

                cache.put("key", new ThinBinaryValue());
            }
        }
    }

    private static class ThinBinaryValueInterceptor extends CacheInterceptorAdapter<String, ThinBinaryValue> {
        @Nullable @Override
        public ThinBinaryValue onBeforePut(Cache.Entry<String, ThinBinaryValue> entry, ThinBinaryValue newVal) {
            return super.onBeforePut(entry, newVal);
        }
    }

    private static class ThinBinaryValue {
    }
}
