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

package org.apache.ignite.springframework.boot.autoconfigure;

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests {@link IgniteClientAutoConfiguration} feature. */
@ExtendWith(SpringExtension.class)
public class IgniteClientAutoConfigureTest {
    /** Default address. */
    public static final String DEFAULT_ADDR = "127.0.0.1:10801";

    /** Cache names. */
    public static final Set<String> CACHES = Collections.singleton("my-cache");

    /** Spring test application context. */
    private ApplicationContextRunner contextRunner;

    /** Server node. */
    private static Ignite node;

    /** */
    @BeforeAll
    public static void beforeClass() {
        node = Ignition.start(new IgniteConfiguration()
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setPort(10801)));

        for (String cache : CACHES)
            node.createCache(cache);
    }

    /** */
    @AfterAll
    public static void afterClass() {
        node.close();
    }

    /** */
    @BeforeEach
    public void beforeTest() {
        contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class));
    }

    /** Tests that Ignite node start without explicit configuration. */
    @Test
    public void testIgniteClientStarts() throws Exception {
        contextRunner
            .withPropertyValues("ignite-client.addresses=" + DEFAULT_ADDR)
            .run(this::checkContext);
    }

    /** Tests that Ignite node will use configuration provided in {@link BeanFactory}. */
    @Test
    public void testIgniteClientUseProvidedConfiguration() throws Exception {
        contextRunner
            .withBean(ClientConfiguration.class, () -> new ClientConfiguration().setAddresses(DEFAULT_ADDR))
            .run(this::checkContext);
    }

    /** Tests that Spring will use {@link IgniteClientConfigurer} to customize {@link ClientConfiguration}. */
    @Test
    public void testIgniteClientConfigurer() throws Exception {
        contextRunner
            .withBean(IgniteClientConfigurer.class, () -> cfg -> cfg.setAddresses(DEFAULT_ADDR))
            .run(this::checkContext);
    }

    /**
     * Tests that application properties will override {@link ClientConfiguration}
     * provided by {@link IgniteClientConfigurer}.
     */
    @Test
    public void testApplicationPropertiesOverridesConfigurer() throws Exception {
        contextRunner
            .withPropertyValues("ignite-client.addresses=" + DEFAULT_ADDR)
            .withBean(IgniteClientConfigurer.class,
                //Intentionally setting wrong connect address.
                //Application property should override this address
                () -> cfg -> cfg.setAddresses("1.1.1.1:8080"))
            .run(this::checkContext);
    }

    /** @param context Context to check */
    private void checkContext(AssertableApplicationContext context) {
        assertThat(context).hasSingleBean(ClientConfiguration.class);
        assertThat(context).hasSingleBean(IgniteClient.class);

        ClientConfiguration cfg = context.getBean(ClientConfiguration.class);

        assertEquals(1, cfg.getAddresses().length);
        assertEquals(DEFAULT_ADDR, cfg.getAddresses()[0]);

        IgniteClient cli = context.getBean(IgniteClient.class);

        assertTrue(CACHES.containsAll(cli.cacheNames()));
    }
}
