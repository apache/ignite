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

package org.apache.ignite.rest.presentation;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.rest.presentation.hocon.HoconPresentation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Testing the {@link ConfigurationPresentation}.
 */
public class ConfigurationPresentationTest {
    /** Configuration registry. */
    private static ConfigurationRegistry cfgRegistry;

    /** Configuration presentation. */
    private static ConfigurationPresentation<String> cfgPresentation;

    /** Test root configuration. */
    private static TestRootConfiguration cfg;

    /**  */
    @BeforeAll
    static void beforeAll() {
        Validator<Value, Object> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override public void validate(Value annotation, ValidationContext<Object> ctx) {
                if (Objects.equals("error", ctx.getNewValue()))
                    ctx.addIssue(new ValidationIssue("Error word"));
            }
        };

        cfgRegistry = new ConfigurationRegistry(
            List.of(TestRootConfiguration.KEY),
            Map.of(Value.class, Set.of(validator)),
            new TestConfigurationStorage(LOCAL),
            List.of()
        );

        cfgRegistry.start();

        cfgPresentation = new HoconPresentation(cfgRegistry);

        cfg = cfgRegistry.getConfiguration(TestRootConfiguration.KEY);
    }

    /** */
    @AfterAll
    static void afterAll() {
        cfgRegistry.stop();
        cfgRegistry = null;

        cfgPresentation = null;

        cfg = null;
    }

    /** */
    @BeforeEach
    void beforeEach() throws Exception {
        cfg.change(cfg -> cfg.changeFoo("foo").changeSubCfg(subCfg -> subCfg.changeBar("bar"))).get(1, SECONDS);
    }

    /** */
    @Test
    void testRepresentWholeCfg() {
        String s = "{\"root\":{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"bar\"}}}";

        assertEquals(s, cfgPresentation.represent());
        assertEquals(s, cfgPresentation.representByPath(null));
    }

    /** */
    @Test
    void testCorrectRepresentCfgByPath() {
        assertEquals("{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"bar\"}}", cfgPresentation.representByPath("root"));
        assertEquals("\"foo\"", cfgPresentation.representByPath("root.foo"));
        assertEquals("{\"bar\":\"bar\"}", cfgPresentation.representByPath("root.subCfg"));
        assertEquals("\"bar\"", cfgPresentation.representByPath("root.subCfg.bar"));
    }

    /** */
    @Test
    void testErrorRepresentCfgByPath() {
        assertThrows(
            IllegalArgumentException.class,
            () -> cfgPresentation.representByPath(UUID.randomUUID().toString())
        );
    }

    /** */
    @Test
    void testCorrectUpdateFullCfg() {
        String updateVal = "{\"root\":{\"foo\":\"bar\",\"subCfg\":{\"bar\":\"foo\"}}}";

        cfgPresentation.update(updateVal);

        assertEquals("bar", cfg.foo().value());
        assertEquals("foo", cfg.subCfg().bar().value());
        assertEquals(updateVal, cfgPresentation.represent());
    }

    /** */
    @Test
    void testCorrectUpdateSubCfg() {
        cfgPresentation.update("{\"root\":{\"subCfg\":{\"bar\":\"foo\"}}}");

        assertEquals("foo", cfg.foo().value());
        assertEquals("foo", cfg.subCfg().bar().value());
        assertEquals("{\"root\":{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"foo\"}}}", cfgPresentation.represent());
    }

    /** */
    @Test
    void testErrorUpdateCfg() {
        assertThrows(
            IllegalArgumentException.class,
            () -> cfgPresentation.update("{\"root\":{\"foo\":100,\"subCfg\":{\"bar\":\"foo\"}}}")
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> cfgPresentation.update("{\"root0\":{\"foo\":\"foo\",\"subCfg\":{\"bar\":\"foo\"}}}")
        );

        assertThrows(IllegalArgumentException.class, () -> cfgPresentation.update("{"));

        assertThrows(IllegalArgumentException.class, () -> cfgPresentation.update(""));

        assertThrows(
            ConfigurationValidationException.class,
            () -> cfgPresentation.update("{\"root\":{\"foo\":\"error\",\"subCfg\":{\"bar\":\"foo\"}}}")
        );
    }

    /**
     * Test root configuration schema.
     */
    @ConfigurationRoot(rootName = "root")
    public static class TestRootConfigurationSchema {
        /** Foo field. */
        @Value(hasDefault = true)
        public String foo = "foo";

        /** Sub configuration schema. */
        @ConfigValue
        public TestSubConfigurationSchema subCfg;
    }

    /**
     * Test sub configuration schema.
     */
    @Config
    public static class TestSubConfigurationSchema {
        /** Bar field. */
        @Value(hasDefault = true)
        public String bar = "bar";
    }
}
