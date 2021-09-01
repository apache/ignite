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
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
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
                List.of(ExtendedFirstRootConfigurationSchema.class)
            )
        );

        // Check that everything is fine.
        new ConfigurationRegistry(
            List.of(FirstRootConfiguration.KEY, SecondRootConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of(ExtendedFirstRootConfigurationSchema.class)
        );
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
}
