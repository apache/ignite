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

package org.apache.ignite.rest.presentation.json;

import com.google.gson.JsonNull;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static com.google.gson.JsonParser.parseString;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.rest.presentation.json.JsonConverter.jsonSource;
import static org.apache.ignite.rest.presentation.json.JsonConverter.jsonVisitor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
public class JsonConverterTest {
    /** */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class JsonRootConfigurationSchema {
        /** */
        @NamedConfigValue
        public JsonArraysConfigurationSchema arraysList;

        /** */
        @NamedConfigValue
        public JsonPrimitivesConfigurationSchema primitivesList;
    }

    /** */
    @Config
    public static class JsonArraysConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public boolean[] booleans = {false};

        /** */
        @Value(hasDefault = true)
        public int[] ints = {0};

        /** */
        @Value(hasDefault = true)
        public long[] longs = {0L};

        /** */
        @Value(hasDefault = true)
        public double[] doubles = {0d};

        /** */
        @Value(hasDefault = true)
        public String[] strings = {""};
    }

    /** */
    @Config
    public static class JsonPrimitivesConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public boolean booleanVal = false;

        /** */
        @Value(hasDefault = true)
        public int intVal = 0;

        /** */
        @Value(hasDefault = true)
        public long longVal = 0L;

        /** */
        @Value(hasDefault = true)
        public double doubleVal = 0d;

        /** */
        @Value(hasDefault = true)
        public String stringVal = "";
    }

    /** */
    private ConfigurationRegistry registry;

    /** */
    private JsonRootConfiguration configuration;

    /** */
    @BeforeEach
    public void before() {
        registry = new ConfigurationRegistry(
            Collections.singletonList(JsonRootConfiguration.KEY),
            Collections.emptyMap(),
            Collections.singletonList(new TestConfigurationStorage())
        );

        configuration = registry.getConfiguration(JsonRootConfiguration.KEY);
    }

    /** */
    @AfterEach
    public void after() {
        registry.stop();
    }

    /** */
    @Test
    public void toJsonBasic() throws Exception {
        assertEquals(
            parseString("{'root':{'arraysList':{},'primitivesList':{}}}"),
            registry.represent(emptyList(), jsonVisitor())
        );

        assertEquals(
            parseString("{'arraysList':{},'primitivesList':{}}"),
            registry.represent(List.of("root"), jsonVisitor())
        );

        assertEquals(
            parseString("{}"),
            registry.represent(List.of("root", "arraysList"), jsonVisitor())
        );

        IllegalArgumentException e
            = assertThrows(IllegalArgumentException.class, () -> registry.represent(List.of("doot"), jsonVisitor()));
        assertEquals("Configuration 'doot' is not found", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () -> registry.represent(List.of("root", "x"), jsonVisitor()));
        assertEquals("Configuration 'root.x' is not found", e.getMessage());

        assertEquals(
            JsonNull.INSTANCE,
            registry.represent(List.of("root", "primitivesList", "foo"), jsonVisitor())
        );
    }

    /** */
    @Test
    public void toJsonPrimitives() throws Exception {
        configuration.change(cfg -> cfg
            .changePrimitivesList(primitivesList -> primitivesList
                .create("name", primitives -> {})
            )
        ).get(1, SECONDS);

        assertEquals(
            parseString("{'booleanVal':false,'intVal':0,'longVal':0,'doubleVal':0.0,'stringVal':''}"),
            registry.represent(List.of("root", "primitivesList", "name"), jsonVisitor())
        );

        assertEquals(
            parseString("false"),
            registry.represent(List.of("root", "primitivesList", "name", "booleanVal"), jsonVisitor())
        );

        assertEquals(
            parseString("0"),
            registry.represent(List.of("root", "primitivesList", "name", "intVal"), jsonVisitor())
        );

        assertEquals(
            parseString("0"),
            registry.represent(List.of("root", "primitivesList", "name", "longVal"), jsonVisitor())
        );

        assertEquals(
            parseString("0.0"),
            registry.represent(List.of("root", "primitivesList", "name", "doubleVal"), jsonVisitor())
        );

        assertEquals(
            parseString("''"),
            registry.represent(List.of("root", "primitivesList", "name", "stringVal"), jsonVisitor())
        );
    }

    /** */
    @Test
    public void toJsonArrays() throws Exception {
        configuration.change(cfg -> cfg
            .changeArraysList(arraysList -> arraysList
                .create("name", arrays -> {})
            )
        ).get(1, SECONDS);

        assertEquals(
            parseString("{'name':{'booleans':[false],'ints':[0],'longs':[0],'doubles':[0.0],'strings':['']}}"),
            registry.represent(List.of("root", "arraysList"), jsonVisitor())
        );

        assertEquals(
            parseString("[false]"),
            registry.represent(List.of("root", "arraysList", "name", "booleans"), jsonVisitor())
        );

        assertEquals(
            parseString("[0]"),
            registry.represent(List.of("root", "arraysList", "name", "ints"), jsonVisitor())
        );

        assertEquals(
            parseString("[0]"),
            registry.represent(List.of("root", "arraysList", "name", "longs"), jsonVisitor())
        );

        assertEquals(
            parseString("[0.0]"),
            registry.represent(List.of("root", "arraysList", "name", "doubles"), jsonVisitor())
        );

        assertEquals(
            parseString("['']"),
            registry.represent(List.of("root", "arraysList", "name", "strings"), jsonVisitor())
        );
    }

    /** */
    @Test
    public void fromJsonBasic() throws Exception {
        // Wrong names:
        assertThrowsIllegalArgException(
            () -> change("{'doot' : {}}"),
            "'doot' configuration root doesn't exist"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'foo' : {}}}"),
            "'root' configuration doesn't have 'foo' subconfiguration"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList':{'name':{'x' : 1}}}}"),
            "'root.arraysList.name' configuration doesn't have 'x' subconfiguration"
        );

        // Wrong node types:
        assertThrowsIllegalArgException(
            () -> change("{'root' : 'foo'}"),
            "'root' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList' : 'foo'}}"),
            "'root.arraysList' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList':{'name' : 'foo'}}}"),
            "'root.arraysList.name' is expected to be a composite configuration node, not a single value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList':{'name':{'ints' : {}}}}}"),
            "'int[]' is expected as a type for 'root.arraysList.name.ints' configuration value"
        );
    }

    /** */
    @Test
    public void fromJsonPrimitives() throws Exception {
        change("{'root':{'primitivesList':{'name' : {}}}}");

        JsonPrimitivesConfiguration primitives = configuration.primitivesList().get("name");
        assertNotNull(primitives);

        change("{'root':{'primitivesList':{'name':{'booleanVal' : true}}}}");
        assertTrue(primitives.booleanVal().value());

        change("{'root':{'primitivesList':{'name':{'intVal' : 12345}}}}");
        assertEquals(12345, primitives.intVal().value());

        change("{'root':{'primitivesList':{'name':{'longVal' : 12345678900}}}}");
        assertEquals(12345678900L, primitives.longVal().value());

        change("{'root':{'primitivesList':{'name':{'doubleVal' : 2.5}}}}");
        assertEquals(2.5d, primitives.doubleVal().value());

        change("{'root':{'primitivesList':{'name':{'stringVal' : 'foo'}}}}");
        assertEquals("foo", primitives.stringVal().value());

        // Wrong value types:
        assertThrowsIllegalArgException(
            () -> change("{'root':{'primitivesList':{'name':{'booleanVal' : 'true'}}}}"),
            "'boolean' is expected as a type for 'root.primitivesList.name.booleanVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'primitivesList':{'name':{'intVal' : 12345678900}}}}"),
            "'root.primitivesList.name.intVal' has integer type and the value 12345678900 is out of bounds"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'primitivesList':{'name':{'intVal' : false}}}}"),
            "'int' is expected as a type for 'root.primitivesList.name.intVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'primitivesList':{'name':{'stringVal' : 10}}}}"),
            "'String' is expected as a type for 'root.primitivesList.name.stringVal' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'primitivesList':{'name':{'doubleVal' : []}}}}"),
            "'double' is expected as a type for 'root.primitivesList.name.doubleVal' configuration value"
        );
    }

    /** */
    @Test
    public void fromJsonArrays() throws Exception {
        change("{'root':{'arraysList':{'name' : {}}}}");

        JsonArraysConfiguration arrays = configuration.arraysList().get("name");
        assertNotNull(arrays);

        change("{'root':{'arraysList':{'name':{'booleans' : [true]}}}}");
        assertArrayEquals(new boolean[] {true}, arrays.booleans().value());

        change("{'root':{'arraysList':{'name':{'ints' : [12345]}}}}");
        assertArrayEquals(new int[] {12345}, arrays.ints().value());

        change("{'root':{'arraysList':{'name':{'longs' : [12345678900]}}}}");
        assertArrayEquals(new long[] {12345678900L}, arrays.longs().value());

        change("{'root':{'arraysList':{'name':{'doubles' : [2.5]}}}}");
        assertArrayEquals(new double[] {2.5d}, arrays.doubles().value());

        change("{'root':{'arraysList':{'name':{'strings' : ['foo']}}}}");
        assertArrayEquals(new String[] {"foo"}, arrays.strings().value());

        // Wrong value types:
        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList':{'name':{'booleans' : true}}}}"),
            "'boolean[]' is expected as a type for 'root.arraysList.name.booleans' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList':{'name':{'ints' : ['0',0]}}}}"),
            "'int' is expected as a type for 'root.arraysList.name.ints[0]' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList':{'name':{'longs' : [0,'0']}}}}"),
            "'long' is expected as a type for 'root.arraysList.name.longs[1]' configuration value"
        );

        assertThrowsIllegalArgException(
            () -> change("{'root':{'arraysList':{'name':{'booleans' : [{}]}}}}"),
            "'boolean' is expected as a type for 'root.arraysList.name.booleans[0]' configuration value"
        );
    }

    /** */
    private void change(String json) throws Exception {
        try {
            registry.change(jsonSource(parseString(json)), null).get(1, SECONDS);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof Exception)
                throw (Exception)cause;

            throw e;
        }
    }

    /** */
    private static void assertThrowsIllegalArgException(Executable executable, String msg) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, executable);

        assertEquals(msg, e.getMessage());
    }
}
