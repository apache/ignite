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

package org.apache.ignite.internal.configuration.asm;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.addDefaults;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Testing the {@link ConfigurationAsmGenerator}.
 */
public class ConfigurationAsmGeneratorTest {
    /** Configuration changer. */
    private static ConfigurationChanger changer;

    /** Configuration generator. */
    private static ConfigurationAsmGenerator generator;

    /** */
    @BeforeAll
    public static void beforeAll() {
        Collection<Class<?>> extensions = List.of(
            ExtendedTestRootConfigurationSchema.class,
            ExtendedSecondTestRootConfigurationSchema.class,
            ExtendedTestConfigurationSchema.class,
            ExtendedSecondTestConfigurationSchema.class
        );

        generator = new ConfigurationAsmGenerator();

        changer = new TestConfigurationChanger(
            generator,
            List.of(TestRootConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            extensions
        );

        changer.start();
        changer.initializeDefaults();
    }

    /** */
    @AfterAll
    public static void after() {
        changer.stop();

        generator = null;
        changer = null;
    }

    /** */
    @Test
    void testExtendedRootConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration baseRootConfig = (TestRootConfiguration)config;

        ExtendedTestRootConfiguration extendedRootConfig = (ExtendedTestRootConfiguration)config;

        ExtendedSecondTestRootConfiguration extendedSecondRootConfig = (ExtendedSecondTestRootConfiguration)config;

        assertSame(baseRootConfig.i0(), extendedRootConfig.i0());
        assertSame(baseRootConfig.i0(), extendedSecondRootConfig.i0());

        assertSame(baseRootConfig.str0(), extendedRootConfig.str0());
        assertSame(baseRootConfig.str0(), extendedSecondRootConfig.str0());

        assertSame(baseRootConfig.subCfg(), extendedRootConfig.subCfg());
        assertSame(baseRootConfig.subCfg(), extendedSecondRootConfig.subCfg());

        assertSame(baseRootConfig.namedCfg(), extendedRootConfig.namedCfg());
        assertSame(baseRootConfig.namedCfg(), extendedSecondRootConfig.namedCfg());

        assertNotNull(extendedSecondRootConfig.i1());

        // Check view and change interfaces.

        assertTrue(baseRootConfig.value() instanceof ExtendedTestRootView);
        assertTrue(baseRootConfig.value() instanceof ExtendedSecondTestRootView);

        assertSame(baseRootConfig.value(), extendedRootConfig.value());
        assertSame(baseRootConfig.value(), extendedSecondRootConfig.value());

        baseRootConfig.change(c -> {
            assertTrue(c instanceof ExtendedTestRootChange);
            assertTrue(c instanceof ExtendedSecondTestRootChange);

            c.changeI0(10).changeStr0("str0");

            ((ExtendedTestRootChange)c).changeStr1("str1").changeStr0("str0");
            ((ExtendedSecondTestRootChange)c).changeI1(200).changeStr0("str0");
        }).get(1, SECONDS);
    }

    /** */
    @Test
    void testExtendedSubConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration rootConfig = (TestRootConfiguration)config;

        TestConfiguration baseSubConfig = rootConfig.subCfg();

        ExtendedTestConfiguration extendedSubConfig = (ExtendedTestConfiguration)rootConfig.subCfg();

        ExtendedSecondTestConfiguration extendedSecondSubConfig =
            (ExtendedSecondTestConfiguration)rootConfig.subCfg();

        assertSame(baseSubConfig.i0(), extendedSubConfig.i0());
        assertSame(baseSubConfig.i0(), extendedSecondSubConfig.i0());

        assertSame(baseSubConfig.str2(), extendedSubConfig.str2());
        assertSame(baseSubConfig.str2(), extendedSecondSubConfig.str2());

        assertNotNull(extendedSecondSubConfig.i1());

        // Check view and change interfaces.

        assertTrue(baseSubConfig.value() instanceof ExtendedTestView);
        assertTrue(baseSubConfig.value() instanceof ExtendedSecondTestView);

        assertSame(baseSubConfig.value(), extendedSubConfig.value());
        assertSame(baseSubConfig.value(), extendedSecondSubConfig.value());

        baseSubConfig.change(c -> {
            assertTrue(c instanceof ExtendedTestChange);
            assertTrue(c instanceof ExtendedSecondTestChange);

            c.changeI0(10).changeStr2("str2");

            ((ExtendedTestChange)c).changeStr3("str3").changeStr2("str2");
            ((ExtendedSecondTestChange)c).changeI1(200).changeStr2("str2");
        }).get(1, SECONDS);
    }

    /** */
    @Test
    void testExtendedNamedConfiguration() throws Exception {
        DynamicConfiguration<?, ?> config = generator.instantiateCfg(TestRootConfiguration.KEY, changer);

        TestRootConfiguration rootConfig = (TestRootConfiguration)config;

        String key = UUID.randomUUID().toString();

        rootConfig.namedCfg().change(c -> c.create(key, c0 -> c0.changeI0(0).changeStr2("foo2"))).get(1, SECONDS);

        TestConfiguration namedConfig = rootConfig.namedCfg().get(key);

        assertTrue(namedConfig instanceof ExtendedTestConfiguration);
        assertTrue(namedConfig instanceof ExtendedSecondTestConfiguration);

        // Check view and change interfaces.

        assertTrue(namedConfig.value() instanceof ExtendedTestView);
        assertTrue(namedConfig.value() instanceof ExtendedSecondTestView);

        namedConfig.change(c -> {
            assertTrue(c instanceof ExtendedTestChange);
            assertTrue(c instanceof ExtendedSecondTestChange);

            c.changeStr2("str2").changeI0(10);

            ((ExtendedTestChange)c).changeStr3("str3").changeStr2("str2");
            ((ExtendedSecondTestChange)c).changeI1(100).changeStr2("str2");
        }).get(1, SECONDS);
    }

    /** */
    @Test
    void testConstructInternalConfig() {
        InnerNode innerNode = generator.instantiateNode(TestRootConfiguration.KEY.schemaClass());

        addDefaults(innerNode);

        InnerNode subInnerNode = (InnerNode)((TestRootView)innerNode).subCfg();

        // Check that no fields for internal configuration will be changed.

        assertThrows(NoSuchElementException.class, () -> innerNode.construct("str1", null, false));
        assertThrows(NoSuchElementException.class, () -> innerNode.construct("i1", null, false));

        assertThrows(NoSuchElementException.class, () -> subInnerNode.construct("str3", null, false));
        assertThrows(NoSuchElementException.class, () -> subInnerNode.construct("i1", null, false));

        // Check that fields for internal configuration will be changed.

        innerNode.construct("str1", null, true);
        innerNode.construct("i1", null, true);

        subInnerNode.construct("str3", null, true);
        subInnerNode.construct("i1", null, true);
    }

    /**
     * Test root configuration schema.
     */
    @ConfigurationRoot(rootName = "test")
    public static class TestRootConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i0 = 0;

        /** String field. */
        @Value(hasDefault = true)
        public String str0 = "str0";

        /** Sub configuration field. */
        @ConfigValue
        public TestConfigurationSchema subCfg;

        /** Named configuration field. */
        @NamedConfigValue
        public TestConfigurationSchema namedCfg;
    }

    /**
     * Extending the {@link TestRootConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedTestRootConfigurationSchema extends TestRootConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str1 = "str1";
    }

    /**
     * Extending the {@link TestRootConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedSecondTestRootConfigurationSchema extends TestRootConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i1 = 0;
    }

    /**
     * Test configuration schema.
     */
    @Config
    public static class TestConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i0 = 0;

        /** String field. */
        @Value(hasDefault = true)
        public String str2 = "str2";
    }

    /**
     * Extending the {@link TestConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedTestConfigurationSchema extends TestConfigurationSchema {
        /** String field. */
        @Value(hasDefault = true)
        public String str3 = "str3";
    }

    /**
     * Extending the {@link TestConfigurationSchema}.
     */
    @InternalConfiguration
    public static class ExtendedSecondTestConfigurationSchema extends TestConfigurationSchema {
        /** Integer field. */
        @Value(hasDefault = true)
        public int i1 = 0;
    }
}
