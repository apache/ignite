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

package org.apache.ignite.internal.processors.security.service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.Test;

/** */
public class ServiceStaticConfigTest extends AbstractSecurityTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName("CounterService");
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setTotalCount(1);
        srvcCfg.setService(new CounterServiceImpl());

        cfg.setServiceConfiguration(srvcCfg);

        return cfg;
    }

    /** */
    @Test
    public void testNodeStarted() throws Exception {
        startGrid(0);
        startGrid(1);

        assertEquals(2, G.allGrids().size());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    public static class CounterServiceImpl implements Service {
        /** Cntr. */
        private final AtomicInteger cntr = new AtomicInteger();

        /** Is started. */
        volatile boolean isStarted;

        /** {@inheritDoc} */
        @Override public void init() throws Exception {
            isStarted = true;
        }

        /** {@inheritDoc} */
        @Override public void execute() throws Exception {
            while (isStarted) {
                cntr.incrementAndGet();

                TimeUnit.SECONDS.sleep(1);
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            isStarted = false;
        }
    }
}
