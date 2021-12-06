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
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.PartialIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationModule;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CoreDistributedConfigurationModule}.
 */
class CoreDistributedConfigurationModuleTest {
    private final CoreDistributedConfigurationModule module = new CoreDistributedConfigurationModule();

    @Test
    void typeIsDistributed() {
        assertThat(module.type(), is(DISTRIBUTED));
    }

    @Test
    void hasClusterConfigurationRoot() {
        assertThat(module.rootKeys(), hasItem(ClusterConfiguration.KEY));
    }

    @Test
    void hasTablesConfigurationRoot() {
        assertThat(module.rootKeys(), hasItem(TablesConfiguration.KEY));
    }

    @Test
    void hasDataStorageConfigurationRoot() {
        assertThat(module.rootKeys(), hasItem(DataStorageConfiguration.KEY));
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
    void providesHashIndexConfigurationSchemaAsPolymorphicExtension() {
        assertThat(module.polymorphicSchemaExtensions(), hasItem(HashIndexConfigurationSchema.class));
    }

    @Test
    void providesSortedIndexConfigurationSchemaAsPolymorphicExtension() {
        assertThat(module.polymorphicSchemaExtensions(), hasItem(SortedIndexConfigurationSchema.class));
    }

    @Test
    void providesPartialIndexConfigurationSchemaAsPolymorphicExtension() {
        assertThat(module.polymorphicSchemaExtensions(), hasItem(PartialIndexConfigurationSchema.class));
    }

    @Test
    void isLoadedByServiceLoader() {
        Optional<ConfigurationModule> maybeModule = ServiceLoader.load(ConfigurationModule.class).stream()
                .map(Provider::get)
                .filter(module -> module instanceof CoreDistributedConfigurationModule)
                .findAny();

        assertThat(maybeModule, isPresent());
    }
}
