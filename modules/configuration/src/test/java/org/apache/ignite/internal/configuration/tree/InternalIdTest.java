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

package org.apache.ignite.internal.configuration.tree;


import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.InternalId;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.direct.DirectPropertiesTest;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link InternalId}. Doesn't check everything, fair bit of its functionality is covered in {@link DirectPropertiesTest}.
 */
public class InternalIdTest {
    /** Parent configuration for the test. has a single extension and list of polymorphic configurations. */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class InternalIdParentConfigurationSchema {
        @NamedConfigValue
        public InternalIdPolymorphicConfigurationSchema polymorphic;
    }

    /** Internal extension for the parent configuration. */
    @InternalConfiguration
    public static class InternalIdInternalConfigurationSchema extends InternalIdParentConfigurationSchema {
        @InternalId
        public UUID id;
    }

    /** Schema for the polumorphic configuration. */
    @PolymorphicConfig
    public static class InternalIdPolymorphicConfigurationSchema {
        @PolymorphicId
        public String type;

        @InternalId
        public UUID id;
    }

    /** Single polymorhic extension. */
    @PolymorphicConfigInstance("foo")
    public static class InternalIdFooConfigurationSchema extends InternalIdPolymorphicConfigurationSchema {
    }

    private final ConfigurationRegistry registry = new ConfigurationRegistry(
            List.of(InternalIdParentConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of(InternalIdInternalConfigurationSchema.class),
            List.of(InternalIdFooConfigurationSchema.class)
    );

    @BeforeEach
    void setUp() {
        registry.start();

        registry.initializeDefaults();
    }

    @AfterEach
    void tearDown() {
        registry.stop();
    }

    /**
     * Tests that internal id, declared in internal extension, works properly.
     */
    @Test
    public void testInternalExtension() {
        InternalIdParentConfiguration cfg = registry.getConfiguration(InternalIdParentConfiguration.KEY);

        UUID internalId = UUID.randomUUID();

        // Put it there manually, this simplifies the test.
        ((InnerNode) cfg.value()).internalId(internalId);

        // Getting it from the explicit configuration cast should work.
        assertThat(((InternalIdInternalConfiguration) cfg).id().value(), is(equalTo(internalId)));

        // Getting it from the explicit configuration value cast should work as well.
        assertThat(((InternalIdInternalView) cfg.value()).id(), is(equalTo(internalId)));
    }

    /**
     * Tests that internal id, decared in polymorphic configuration, works properly.
     */
    @Test
    public void testPolymorphicExtension() throws Exception {
        InternalIdParentConfiguration cfg = registry.getConfiguration(InternalIdParentConfiguration.KEY);

        // Create polymorphic instance.
        cfg.polymorphic().change(list -> list.create("a", element -> {
            // Check that id is accessible via "raw" instance.
            UUID internalId = element.id();

            assertThat(internalId, is(notNullValue()));

            // Check that id is accessible via "specific" instance.
            InternalIdFooChange foo = element.convert(InternalIdFooChange.class);

            assertThat(foo.id(), is(equalTo(internalId)));
        })).get(1, TimeUnit.SECONDS);

        // Read internal id from the named list directly.
        var list = (NamedListNode<InternalIdPolymorphicView>) cfg.polymorphic().value();

        UUID internalId = list.internalId("a");

        // Check that this internal id matches the one from raw InnerNode list element.
        assertThat(list.getInnerNode("a").internalId(), is(equalTo(internalId)));

        // Check that internal id mathes the one from "specific" configuration view instance.
        assertThat(list.get("a").id(), is(equalTo(internalId)));

        // Check that intetnal id is accessible from the polymorphic Configuration instance.
        assertThat(cfg.polymorphic().get("a").id().value(), is(equalTo(internalId)));
        assertThat(cfg.polymorphic().get("a").value().id(), is(equalTo(internalId)));
    }
}
