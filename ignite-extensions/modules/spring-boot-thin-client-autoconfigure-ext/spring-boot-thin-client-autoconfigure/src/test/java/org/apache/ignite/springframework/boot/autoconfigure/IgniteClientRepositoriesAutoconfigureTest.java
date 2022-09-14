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
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories;
import org.apache.ignite.springframework.boot.autoconfigure.misc.ObjectRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link IgniteClientRepositoryAutoConfiguration} feature. */
@ExtendWith(SpringExtension.class)
public class IgniteClientRepositoriesAutoconfigureTest {
    /** Spring test application context. */
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withPropertyValues("ignite-client.addresses=127.0.0.1:10801")
        .withConfiguration(AutoConfigurations.of(IgniteClientAutoConfiguration.class, IgniteClientRepositoryAutoConfiguration.class));

    /** Server node. */
    private static Ignite node;

    /** Test cache name */
    private static final String CACHE_NAME = "objectCache";

    /** */
    @BeforeAll
    public static void beforeClass() {
        node = Ignition.start(new IgniteConfiguration()
            .setClientConnectorConfiguration(
                new ClientConnectorConfiguration().setPort(10801)));
        node.createCache(CACHE_NAME);
    }

    /** */
    @AfterAll
    public static void afterClass() {
        node.close();
    }

    /** Test default autoconfiguration */
    @Test
    public void testDefaultConfiguration() {
        contextRunner.withUserConfiguration(TestConfiguration.class).run((context) -> {
            assertThat(context).hasSingleBean(ObjectRepository.class);
        });
    }

    /** Test configuration with @EnableIgniteRepositories */
    @Test
    public void testOverrideConfiguration() {
        contextRunner.withUserConfiguration(OverrideConfiguration.class).run((context) -> {
            assertThat(context).hasSingleBean(ObjectRepository.class);
        });
    }

    /** Test configuration with @EnableIgniteRepositories and invalid base package */
    @Test
    public void testInvalidBasePackage() {
        contextRunner.withUserConfiguration(InvalidConfiguration.class).run((context) -> {
            assertThat(context).doesNotHaveBean(ObjectRepository.class);
        });
    }

    /** */
    @AutoConfigurationPackage
    protected static class TestConfiguration {

    }

    /** */
    @EnableIgniteRepositories
    protected static class OverrideConfiguration {

    }

    /** */
    @EnableIgniteRepositories(basePackages = "wrong.package")
    protected static class InvalidConfiguration {

    }
}
