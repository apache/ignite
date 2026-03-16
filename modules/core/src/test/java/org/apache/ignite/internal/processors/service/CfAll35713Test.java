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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class CfAll35713Test extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NODE_NAME = "serviceNode";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            // Имеем кластер, в конфигурации которого задан сервис, который должен быть развёрнут только на узле c ID
            // "serviceNode". serviceNode пока не в кластере.
            cfg.setConsistentId(SERVICE_NODE_NAME)
                .setServiceConfiguration(new ServiceConfiguration()
                    .setName(Greeter.class.getSimpleName())
                    .setService(new GreeterService())
                    .setMaxPerNodeCount(1)
                    .setNodeFilter(new ServiceNodeFilter()));
        }


        return cfg;
    }

    /**
     * Сервисный узел получает свой сервис из кластера при присоединении к кластеру.
     */
    @Test
    public void nodeGetsServiceFromCluster() throws Exception {
        // Когда serviceNode с пустой конфигурацией входит в кластер
        try (IgniteEx srv = startGrid(0); IgniteEx cli = startClientGrid(1)) {
            // И через некоторое время (в реальности порядка 30 секунд) после входа вызывает сервис
            Thread.sleep(2_000);
            final var greeter = cli.services(cli.cluster().forLocal()).serviceProxy(Greeter.class.getSimpleName(), Greeter.class, false);

            // Тогда:
            // Ожидаемый и фактический результат на Ignite SE 16.1.3: вызов проходит успешно
            // Фактический результат на Ignite SE 17.0.0: IgniteException: Failed to find deployed service: Greeter
            assertEquals("Hello", greeter.greet());
        }
    }

    /** */
    public interface Greeter {
        /** */
        String greet();
    }

    /** */
    private static class GreeterService implements Greeter, Service {
        /** */
        @Override public String greet() {
            return "Hello";
        }
    }

    /** */
    public static class ServiceNodeFilter implements IgnitePredicate<ClusterNode> {
        /** */
        @Override public boolean apply(ClusterNode clusterNode) {
            return SERVICE_NODE_NAME.equals(clusterNode.consistentId());
        }
    }
}
