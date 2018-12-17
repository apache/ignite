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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteSpringBean;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link org.apache.ignite.IgniteSpringBean} serialization.
 */
@RunWith(JUnit4.class)
public class GridSpringBeanSerializationSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Marshaller. */
    private Marshaller marsh;

    /** Attribute key. */
    private static final String ATTR_KEY = "checkAttr";

    /** Bean. */
    private IgniteSpringBean bean;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteConfiguration cfg = config();

        marsh = createStandaloneBinaryMarshaller(cfg);

        bean = new IgniteSpringBean();

        bean.setConfiguration(cfg);

        bean.afterSingletonsInstantiated();
    }

    /**
     * @return Grid configuration.
     */
    private IgniteConfiguration config() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setUserAttributes(F.asMap(ATTR_KEY, true));

        cfg.setConnectorConfiguration(null);

        cfg.setIgniteInstanceName(getTestIgniteInstanceName());

        cfg.setBinaryConfiguration(new BinaryConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        bean.destroy();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerialization() throws Exception {
        assert bean != null;

        IgniteSpringBean bean0 = marsh.unmarshal(marsh.marshal(bean), null);

        assert bean0 != null;
        assert bean0.log() != null;
        assert bean0.cluster().localNode() != null;
        assert bean0.cluster().localNode().<Boolean>attribute(ATTR_KEY);
    }
}
