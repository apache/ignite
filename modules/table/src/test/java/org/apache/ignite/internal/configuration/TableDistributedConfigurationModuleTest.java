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

package org.apache.ignite.internal.configuration;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import java.util.ServiceLoader;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TableDistributedConfigurationModule}.
 */
class TableDistributedConfigurationModuleTest {
    private final TableDistributedConfigurationModule module = new TableDistributedConfigurationModule();

    @Test
    void typeIsDistributed() {
        assertThat(module.type(), is(DISTRIBUTED));
    }

    @Test
    void hasNoConfigurationRoots() {
        assertThat(module.rootKeys(), is(empty()));
    }

    @Test
    void providesNoValidators() {
        assertThat(module.validators(), is(anEmptyMap()));
    }

    @Test
    void providesExternalTableConfigurationSchemaAsInternalExtension() {
        assertThat(module.internalSchemaExtensions(), hasItem(ExtendedTableConfigurationSchema.class));
    }

    @Test
    void providesNoPolymorphicSchemaExtensions() {
        assertThat(module.polymorphicSchemaExtensions(), is(empty()));
    }

    @Test
    void isLoadedByServiceLoader() {
        Optional<ConfigurationModule> maybeModule = ServiceLoader.load(ConfigurationModule.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(module -> module instanceof TableDistributedConfigurationModule)
                .findAny();

        assertThat(maybeModule, isPresent());
    }
}
