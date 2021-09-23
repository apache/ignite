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

package org.apache.ignite.internal.configuration.hocon;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.hocon.HoconConverter.hoconSource;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the {@link HoconConverter}.
 */
public class HoconConverterTest {
    /** */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class HoconRootConfigurationSchema {
        /** */
        @NamedConfigValue(syntheticKeyName = "a")
        public HoconArraysConfigurationSchema arraysList;

        /** */
        @NamedConfigValue(syntheticKeyName = "p")
        public HoconPrimitivesConfigurationSchema primitivesList;
    }

    /**
     * Configuration schema for testing the support of arrays of primitives.
     */
    @Config
    public static class HoconArraysConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public boolean[] booleans = {false};

        /** */
        @Value(hasDefault = true)
        public byte[] bytes = {0};

        /** */
        @Value(hasDefault = true)
        public short[] shorts = {0};

        /** */
        @Value(hasDefault = true)
        public int[] ints = {0};

        /** */
        @Value(hasDefault = true)
        public long[] longs = {0L};

        /** */
        @Value(hasDefault = true)
        public char[] chars = {0};

        /** */
        @Value(hasDefault = true)
        public float[] floats = {0};

        /** */
        @Value(hasDefault = true)
        public double[] doubles = {0};

        /** */
        @Value(hasDefault = true)
        public String[] strings = {""};
    }

    /**
     * Configuration schema for testing the support of primitives.
     */
    @Config
    public static class HoconPrimitivesConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public boolean booleanVal = false;

        /** */
        @Value(hasDefault = true)
        public byte byteVal = 0;

        /** */
        @Value(hasDefault = true)
        public short shortVal = 0;

        /** */
        @Value(hasDefault = true)
        public int intVal = 0;

        /** */
        @Value(hasDefault = true)
        public long longVal = 0L;

        /** */
        @Value(hasDefault = true)
        public char charVal = 0;

        /** */
        @Value(hasDefault = true)
        public float floatVal = 0;

        /** */
        @Value(hasDefault = true)
        public double doubleVal = 0;

        /** */
        @Value(hasDefault = true)
        public String stringVal = "";
    }

    /** */
    private static ConfigurationRegistry registry;

    /** */
    private static HoconRootConfiguration configuration;

    /** */
    @BeforeAll
    public static void beforeAll() {
        registry = new ConfigurationRegistry(
            List.of(HoconRootConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of()
        );

        registry.start();

        configuration = registry.getConfiguration(HoconRootConfiguration.KEY);
    }

    /** */
    @AfterAll
    public static void after() {
        registry.stop();

        registry = null;

        configuration = null;
    }

    /** */
    @BeforeEach
    public void before() throws Exception {
        configuration.change(cfg -> cfg
            .changePrimitivesList(list -> list.namedListKeys().forEach(list::delete))
            .changeArraysList(list -> list.namedListKeys().forEach(list::delete))
        ).get(1, SECONDS);
    }

    /** */
    @Test
    public void toHoconBasic() {
        assertEquals("root{arraysList=[],primitivesList=[]}", asHoconStr(List.of()));

        assertEquals("arraysList=[],primitivesList=[]", asHoconStr(List.of("root")));

        assertEquals("[]", asHoconStr(List.of("root", "arraysList")));

        assertThrowsIllegalArgException(
            () -> HoconConverter.represent(registry, List.of("doot")),
            "Configuration value 'doot' has not been found"
        );

        assertThrowsIllegalArgException(
            () -> HoconConverter.represent(registry, List.of("root", "x")),
            "Configuration value 'root.x' has not been found"
        );

        assertEquals("null", asHoconStr(List.of("root", "primitivesList", "foo")));
    }

    /**
     * Tests that the {@code HoconConverter} supports serialization of Strings and primitives.
     */
    @Test
    public void testHoconPrimitivesSerialization() throws Exception {
        configuration.change(cfg -> cfg
            .changePrimitivesList(primitivesList -> primitivesList
                .create("name", primitives -> {})
            )
        ).get(1, SECONDS);

        var basePath = List.of("root", "primitivesList", "name");

        assertEquals(
            "booleanVal=false,byteVal=0,charVal=\"\\u0000\",doubleVal=0.0,floatVal=0,intVal=0,longVal=0,shortVal=0,stringVal=\"\"",
            asHoconStr(basePath)
        );

        assertEquals("false", asHoconStr(basePath, "booleanVal"));
        assertEquals("0", asHoconStr(basePath, "byteVal"));
        assertEquals("0", asHoconStr(basePath, "shortVal"));
        assertEquals("0", asHoconStr(basePath, "intVal"));
        assertEquals("0", asHoconStr(basePath, "longVal"));
        assertEquals("\"\\u0000\"", asHoconStr(basePath, "charVal"));
        assertEquals("0", asHoconStr(basePath, "floatVal"));
        assertEquals("0.0", asHoconStr(basePath, "doubleVal"));
        assertEquals("\"\"", asHoconStr(basePath, "stringVal"));
    }

    /**
     * Tests that the {@code HoconConverter} supports serialization of arrays of Strings and primitives.
     */
    @Test
    public void testHoconArraysSerialization() throws Exception {
        configuration.change(cfg -> cfg
            .changeArraysList(arraysList -> arraysList
                .create("name", arrays -> {})
            )
        ).get(1, SECONDS);

        var basePath = List.of("root", "arraysList", "name");

        assertEquals(
            "booleans=[false],bytes=[0],chars=[\"\\u0000\"],doubles=[0.0],floats=[0],ints=[0],longs=[0],shorts=[0],strings=[\"\"]",
            asHoconStr(basePath)
        );

        assertEquals("[false]", asHoconStr(basePath, "booleans"));
        assertEquals("[0]", asHoconStr(basePath, "bytes"));
        assertEquals("[0]", asHoconStr(basePath, "shorts"));
        assertEquals("[0]", asHoconStr(basePath, "ints"));
        assertEquals("[0]", asHoconStr(basePath, "longs"));
        assertEquals("[\"\\u0000\"]", asHoconStr(basePath, "chars"));
        assertEquals("[0]", asHoconStr(basePath, "floats"));
        assertEquals("[0.0]", asHoconStr(basePath, "doubles"));
        assertEquals("[\"\"]", asHoconStr(basePath, "strings"));
    }

    /**
     * Retrieves the HOCON configuration located at the given path.
     */
    private String asHoconStr(List<String> basePath, String... path) {
        List<String> fullPath = Stream.concat(basePath.stream(), Arrays.stream(path)).collect(Collectors.toList());

        ConfigValue hoconCfg = HoconConverter.represent(registry, fullPath);

        return hoconCfg.render(ConfigRenderOptions.concise().setJson(false));
    }

    /** */
    @Test
    public void fromHoconBasic() {
        // Wrong names:
        assertThrowsIllegalArgException(
            () -> change("doot : {}"),
            "'doot' configuration root doesn't exist"
        );

        assertThrowsIllegalArgException(
            () -> change("root.foo : {}"),
            "'root' configuration doesn't have the 'foo' sub-configuration"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.x = 1"),
            "'root.arraysList.name' configuration doesn't have the 'x' sub-configuration"
        );

        // Wrong node types:
        assertThrowsIllegalArgException(
            () -> change("root = foo"),
            "'root' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList = foo"),
            "'root.arraysList' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name = foo"),
            "'root.arraysList.name' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.ints = {}"),
            "'int[]' is expected as a type for the 'root.arraysList.name.ints' configuration value"
        );

        // Wrong ordered named list syntax:
        assertThrowsIllegalArgException(
            () -> change("root.arraysList = [1]"),
            "'root.arraysList[0]' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList = [{}]"),
            "'root.arraysList[0].a' configuration value is mandatory and must be a String"
        );
    }

    /**
     * Tests that the {@code HoconConverter} supports deserialization of Strings and primitives.
     */
    @Test
    public void testHoconPrimitivesDeserialization() throws Throwable {
        change("root.primitivesList = [{p = name}]");

        HoconPrimitivesConfiguration primitives = configuration.primitivesList().get("name");
        assertNotNull(primitives);

        change("root.primitivesList.name.booleanVal = true");
        assertThat(primitives.booleanVal().value(), is(true));

        change("root.primitivesList.name.byteVal = 123");
        assertThat(primitives.byteVal().value(), is((byte)123));

        change("root.primitivesList.name.shortVal = 12345");
        assertThat(primitives.shortVal().value(), is((short)12345));

        change("root.primitivesList.name.intVal = 12345");
        assertThat(primitives.intVal().value(), is(12345));

        change("root.primitivesList.name.longVal = 12345678900");
        assertThat(primitives.longVal().value(), is(12345678900L));

        change("root.primitivesList.name.charVal = p");
        assertThat(primitives.charVal().value(), is('p'));

        change("root.primitivesList.name.floatVal = 2.5");
        assertThat(primitives.floatVal().value(), is(2.5f));

        change("root.primitivesList.name.doubleVal = 2.5");
        assertThat(primitives.doubleVal().value(), is(2.5d));

        change("root.primitivesList.name.stringVal = foo");
        assertThat(primitives.stringVal().value(), is("foo"));
    }

    /**
     * Tests deserialization errors that can happen during the deserialization of primitives.
     */
    @Test
    public void testInvalidHoconPrimitivesDeserialization() {
        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.booleanVal = \"true\""),
            "'boolean' is expected as a type for the 'root.primitivesList.name.booleanVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.byteVal = 290"),
            "Value '290' of 'root.primitivesList.name.byteVal' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.byteVal = false"),
            "'byte' is expected as a type for the 'root.primitivesList.name.byteVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.shortVal = 12345678900"),
            "Value '12345678900' of 'root.primitivesList.name.shortVal' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.shortVal = false"),
            "'short' is expected as a type for the 'root.primitivesList.name.shortVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.intVal = 12345678900"),
            "Value '12345678900' of 'root.primitivesList.name.intVal' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.intVal = false"),
            "'int' is expected as a type for the 'root.primitivesList.name.intVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.longVal = false"),
            "'long' is expected as a type for the 'root.primitivesList.name.longVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.charVal = 10"),
            "'char' is expected as a type for the 'root.primitivesList.name.charVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.floatVal = false"),
            "'float' is expected as a type for the 'root.primitivesList.name.floatVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.doubleVal = []"),
            "'double' is expected as a type for the 'root.primitivesList.name.doubleVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.primitivesList.name.stringVal = 10"),
            "'String' is expected as a type for the 'root.primitivesList.name.stringVal' configuration value"
        );
    }

    /**
     * Tests that the {@code HoconConverter} supports deserialization of arrays of Strings and primitives.
     */
    @Test
    public void testHoconArraysDeserialization() throws Throwable {
        change("root.arraysList = [{a = name}]");

        HoconArraysConfiguration arrays = configuration.arraysList().get("name");
        assertNotNull(arrays);

        change("root.arraysList.name.booleans = [true]");
        assertThat(arrays.booleans().value(), is(new boolean[] {true}));

        change("root.arraysList.name.bytes = [123]");
        assertThat(arrays.bytes().value(), is(new byte[] {123}));

        change("root.arraysList.name.shorts = [123]");
        assertThat(arrays.shorts().value(), is(new short[] {123}));

        change("root.arraysList.name.ints = [12345]");
        assertThat(arrays.ints().value(), is(new int[] {12345}));

        change("root.arraysList.name.longs = [12345678900]");
        assertThat(arrays.longs().value(), is(new long[] {12345678900L}));

        change("root.arraysList.name.chars = [p]");
        assertThat(arrays.chars().value(), is(new char[] {'p'}));

        change("root.arraysList.name.floats = [2.5]");
        assertThat(arrays.floats().value(), is(new float[] {2.5f}));

        change("root.arraysList.name.doubles = [2.5]");
        assertThat(arrays.doubles().value(), is(new double[] {2.5d}));

        change("root.arraysList.name.strings = [foo]");
        assertThat(arrays.strings().value(), is(new String[] {"foo"}));
    }

    /**
     * Tests deserialization errors that can happen during the deserialization of arrays of primitives.
     */
    @Test
    public void testInvalidHoconArraysDeserialization() {
        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.booleans = true"),
            "'boolean[]' is expected as a type for the 'root.arraysList.name.booleans' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.booleans = [{}]"),
            "'boolean' is expected as a type for the 'root.arraysList.name.booleans[0]' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.bytes = [123, 290]"),
            "Value '290' of 'root.arraysList.name.bytes[1]' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.bytes = false"),
            "'byte[]' is expected as a type for the 'root.arraysList.name.bytes' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.shorts = [12345678900]"),
            "Value '12345678900' of 'root.arraysList.name.shorts[0]' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.shorts = [123, false]"),
            "'short' is expected as a type for the 'root.arraysList.name.shorts[1]' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.ints = [5, 12345678900]"),
            "Value '12345678900' of 'root.arraysList.name.ints[1]' is out of its declared bounds"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.ints = false"),
            "'int[]' is expected as a type for the 'root.arraysList.name.ints' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.longs = [foo]"),
            "'long' is expected as a type for the 'root.arraysList.name.longs[0]' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.chars = 10"),
            "'char[]' is expected as a type for the 'root.arraysList.name.chars' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.chars = [abc]"),
            "'char' is expected as a type for the 'root.arraysList.name.chars[0]' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.floats = [1.2, foo]"),
            "'float' is expected as a type for the 'root.arraysList.name.floats[1]' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.doubles = foo"),
            "'double[]' is expected as a type for the 'root.arraysList.name.doubles' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("root.arraysList.name.strings = 10"),
            "'String[]' is expected as a type for the 'root.arraysList.name.strings' configuration value"
        );
    }

    /**
     * Updates the configuration using the provided HOCON string.
     */
    private void change(String hocon) throws Throwable {
        try {
            registry.change(hoconSource(ConfigFactory.parseString(hocon).root())).get(1, SECONDS);
        }
        catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    /** */
    private static void assertThrowsIllegalArgException(Executable executable, String msg) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, executable);

        assertThat(e.getMessage(), containsString(msg));
    }
}
