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

package org.apache.ignite.configuration;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationModule;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CoreLocalConfigurationModule}.
 */
class CoreLocalConfigurationModuleTest {
    private final CoreLocalConfigurationModule module = new CoreLocalConfigurationModule();

    @Test
    void typeIsLocal() {
        assertThat(module.type(), is(LOCAL));
    }

    @Test
    void hasNetworkConfigurationRoot() {
        assertThat(module.rootKeys(), hasItem(NetworkConfiguration.KEY));
    }

    @Test
    void hasNodeConfigurationRoot() {
        assertThat(module.rootKeys(), hasItem(NodeConfiguration.KEY));
    }

    @Test
    void hasRestConfigurationRoot() {
        assertThat(module.rootKeys(), hasItem(RestConfiguration.KEY));
    }

    @Test
    void hasClientConnectorConfigurationRoot() {
        assertThat(module.rootKeys(), hasItem(ClientConnectorConfiguration.KEY));
    }

    @Test
    void providesNoValidators() {
        assertThat(module.validators(), is(anEmptyMap()));
    }

    @Test
    void providesNoInternalSchemaExtensions() {
        assertThat(module.internalSchemaExtensions(), is(empty()));
    }

    @Test
    void providesNoPolymorphicSchemaExtensions() {
        assertThat(module.polymorphicSchemaExtensions(), is(empty()));
    }

    @Test
    void isLoadedByServiceLoader() {
        Optional<ConfigurationModule> maybeModule = ServiceLoader.load(ConfigurationModule.class).stream()
                .map(Provider::get)
                .filter(module -> module instanceof CoreLocalConfigurationModule)
                .findAny();

        assertThat(maybeModule, isPresent());
    }
}
