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

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/** Tests {@link IgniteAutoConfiguration} feature. */
@ExtendWith(SpringExtension.class)
public class IgniteAutoconfigureTest {
    /** Spring test application context. */
    private ApplicationContextRunner contextRunner;

    /** */
    @BeforeEach
    public void beforeTest() {
        contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(IgniteAutoConfiguration.class));
    }

    /** Tests that Ignite node start without explicit configuration. */
    @Test
    public void testIgniteStarts() throws Exception {
        contextRunner.run((context) -> {
            assertThat(context).hasSingleBean(IgniteConfiguration.class);
            assertThat(context).hasSingleBean(Ignite.class);
        });
    }

    /** Tests that Ignite node will use configuration provided in {@link BeanFactory}. */
    @Test
    public void testIgniteUseProvidedConfiguration() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setIgniteInstanceName("test-name")
            .setConsistentId("test-id");

        contextRunner.withBean(IgniteConfiguration.class, () -> cfg).run((context) -> {
            assertThat(context).hasSingleBean(IgniteConfiguration.class);
            assertThat(context).hasSingleBean(Ignite.class);

            IgniteConfiguration cfg0 = context.getBean(IgniteConfiguration.class);

            assertEquals("Expecting usage of the configuration from context", cfg.getIgniteInstanceName(),
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", cfg.getConsistentId(),
                cfg0.getConsistentId());

            cfg0 = context.getBean(Ignite.class).configuration();

            assertEquals("Expecting usage of the configuration from context", cfg.getIgniteInstanceName(),
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", cfg.getConsistentId(),
                cfg0.getConsistentId());
        });
    }

    /** Tests that Spring will use {@link IgniteConfigurer} to customize {@link IgniteConfiguration}. */
    @Test
    public void testIgniteConfigurer() throws Exception {
        String instanceName = "configurer-test-name";
        String consistentId = "configurer-test-id";

        ApplicationContextRunner runner = contextRunner.withBean(IgniteConfigurer.class,
            () -> cfg -> {
                cfg.setIgniteInstanceName(instanceName).setConsistentId(consistentId);
            });

        runner.run((context) -> {
            assertThat(context).hasSingleBean(IgniteConfiguration.class);
            assertThat(context).hasSingleBean(Ignite.class);

            IgniteConfiguration cfg0 = context.getBean(IgniteConfiguration.class);

            assertEquals("Expecting usage of the configuration from context", instanceName,
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", consistentId,
                cfg0.getConsistentId());

            cfg0 = context.getBean(Ignite.class).configuration();

            assertEquals("Expecting usage of the configuration from context", instanceName,
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", consistentId,
                cfg0.getConsistentId());
        });
    }

    /** Tests that Spring will use application properties to customize {@link IgniteConfiguration}. */
    @Test
    public void testIgniteUseApplicationProperties() throws Exception {
        ApplicationContextRunner runner = contextRunner.withPropertyValues(
                "ignite.igniteInstanceName=from-property-name",
                "ignite.consistentId=from-property-id");

        String instanceName = "from-property-name";
        String consistentId = "from-property-id";

        runner.run((context) -> {
            assertThat(context).hasSingleBean(IgniteConfiguration.class);
            assertThat(context).hasSingleBean(Ignite.class);

            IgniteConfiguration cfg0 = context.getBean(IgniteConfiguration.class);

            assertEquals("Expecting usage of the configuration from context", instanceName,
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", consistentId,
                cfg0.getConsistentId());

            cfg0 = context.getBean(Ignite.class).configuration();

            assertEquals("Expecting usage of the configuration from context", instanceName,
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", consistentId,
                cfg0.getConsistentId());
        });
    }

    /**
     * Tests that application properties will override {@link IgniteConfiguration} provided by {@link IgniteConfigurer}.
     */
    @Test
    public void testApplicationPropertiesOverridesConfigurer() throws Exception {
        String instanceName = "test-name";
        String consistentId = "from-property-id";

        ApplicationContextRunner runner = contextRunner
            .withPropertyValues("ignite.consistentId=from-property-id")
            .withBean(IgniteConfigurer.class,
                () -> cfg -> cfg.setIgniteInstanceName(instanceName).setConsistentId("test-id"));

        runner.run((context) -> {
            assertThat(context).hasSingleBean(IgniteConfiguration.class);
            assertThat(context).hasSingleBean(Ignite.class);

            IgniteConfiguration cfg0 = context.getBean(IgniteConfiguration.class);

            assertEquals("Expecting usage of the configuration from context", instanceName,
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", consistentId,
                cfg0.getConsistentId());

            cfg0 = context.getBean(Ignite.class).configuration();

            assertEquals("Expecting usage of the configuration from context", instanceName,
                cfg0.getIgniteInstanceName());
            assertEquals("Expecting usage of the configuration from context", consistentId,
                cfg0.getConsistentId());
        });
    }
}
