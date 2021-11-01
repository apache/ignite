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

import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Class for testing the {@link ConfigurationRegistry}.
 */
public class ConfigurationRegistryTest {
    /** */
    @Test
    void testValidationInternalConfigurationExtensions() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigurationRegistry(
                List.of(SecondRootConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(LOCAL),
                List.of(ExtendedFirstRootConfigurationSchema.class),
                List.of()
            )
        );

        // Check that everything is fine.
        ConfigurationRegistry configRegistry = new ConfigurationRegistry(
            List.of(FirstRootConfiguration.KEY, SecondRootConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of(ExtendedFirstRootConfigurationSchema.class),
            List.of()
        );

        configRegistry.stop();
    }

    /** */
    @Test
    void testValidationPolymorphicConfigurationExtensions() {
        // There is a polymorphic extension that is missing from the schema.
        assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigurationRegistry(
                List.of(ThirdRootConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(LOCAL),
                List.of(),
                List.of(Second0PolymorphicConfigurationSchema.class)
            )
        );

        // There are two polymorphic extensions with the same id.
        assertThrows(
            IllegalArgumentException.class,
            () -> new ConfigurationRegistry(
                List.of(ThirdRootConfiguration.KEY),
                Map.of(),
                new TestConfigurationStorage(LOCAL),
                List.of(),
                List.of(First0PolymorphicConfigurationSchema.class, ErrorFirst0PolymorphicConfigurationSchema.class)
            )
        );

        // Check that everything is fine.
        ConfigurationRegistry configRegistry = new ConfigurationRegistry(
            List.of(ThirdRootConfiguration.KEY, FourthRootConfiguration.KEY, FifthRootConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of(),
            List.of(
                First0PolymorphicConfigurationSchema.class,
                First1PolymorphicConfigurationSchema.class,
                Second0PolymorphicConfigurationSchema.class,
                Third0PolymorphicConfigurationSchema.class,
                Third1PolymorphicConfigurationSchema.class
            )
        );

        configRegistry.stop();
    }

    /**
     * First root configuration.
     */
    @ConfigurationRoot(rootName = "first")
    public static class FirstRootConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * First root configuration.
     */
    @ConfigurationRoot(rootName = "second")
    public static class SecondRootConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * First extended root configuration.
     */
    @InternalConfiguration
    public static class ExtendedFirstRootConfigurationSchema extends FirstRootConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String strEx = "str";
    }

    /**
     * Third root configuration.
     */
    @ConfigurationRoot(rootName = "third")
    public static class ThirdRootConfigurationSchema {
        /** First polymorphic configuration scheme */
        @ConfigValue
        public FirstPolymorphicConfigurationSchema polymorphicConfig;
    }

    /**
     * Fourth root configuration.
     */
    @ConfigurationRoot(rootName = "fourth")
    public static class FourthRootConfigurationSchema {
        /** Second polymorphic configuration scheme */
        @ConfigValue
        public SecondPolymorphicConfigurationSchema polymorphicConfig;
    }

    /**
     * Fifth root configuration.
     */
    @ConfigurationRoot(rootName = "fifth")
    public static class FifthRootConfigurationSchema {
        /** Third polymorphic configuration scheme */
        @ConfigValue
        public ThirdPolymorphicConfigurationSchema polymorphicConfig;
    }

    /**
     * Simple first polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class FirstPolymorphicConfigurationSchema {
        /** Polymorphic type id field. */
        @PolymorphicId
        public String typeId;
    }

    /**
     * First {@link FirstPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("first0")
    public static class First0PolymorphicConfigurationSchema extends FirstPolymorphicConfigurationSchema {
    }

    /**
     * Second {@link FirstPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("first1")
    public static class First1PolymorphicConfigurationSchema extends FirstPolymorphicConfigurationSchema {
    }

    /**
     * First error {@link FirstPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("first0")
    public static class ErrorFirst0PolymorphicConfigurationSchema extends FirstPolymorphicConfigurationSchema {
    }

    /**
     * Second polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class SecondPolymorphicConfigurationSchema {
        /** Polymorphic type id field. */
        @PolymorphicId
        public String typeId;
    }

    /**
     * First {@link SecondPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("second0")
    public static class Second0PolymorphicConfigurationSchema extends SecondPolymorphicConfigurationSchema {
    }

    /**
     * Third polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class ThirdPolymorphicConfigurationSchema {
        /** Polymorphic type id field. */
        @PolymorphicId(hasDefault = true)
        public String typeId = "third0";
    }

    /**
     * First {@link ThirdPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("third0")
    public static class Third0PolymorphicConfigurationSchema extends ThirdPolymorphicConfigurationSchema {
    }

    /**
     * First {@link ThirdPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("third1")
    public static class Third1PolymorphicConfigurationSchema extends ThirdPolymorphicConfigurationSchema {
    }
}
