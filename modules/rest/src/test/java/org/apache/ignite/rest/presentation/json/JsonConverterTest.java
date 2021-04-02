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
import java.util.List;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.google.gson.JsonParser.parseString;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.rest.presentation.json.JsonConverter.jsonVisitor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** */
public class JsonConverterTest {
    /** */
    @ConfigurationRoot(rootName = "root", storage = TestConfigurationStorage.class)
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
    private final ConfigurationRegistry registry = new ConfigurationRegistry();

    /** */
    private JsonRootConfiguration configuration;

    /** */
    @BeforeEach
    public void before() {
        registry.registerRootKey(JsonRootConfiguration.KEY);

        registry.registerStorage(new TestConfigurationStorage());

        configuration = registry.getConfiguration(JsonRootConfiguration.KEY);
    }

    /** */
    @AfterEach
    public void after() {
        registry.stop();
    }

    /** */
    @Test
    public void toJson() throws Exception {
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

        configuration.change(cfg -> cfg
            .changeArraysList(arraysList -> arraysList
                .create("name", arrays -> {})
            )
            .changePrimitivesList(primitivesList -> primitivesList
                .create("name", primitives -> {})
            )
        ).get(1, SECONDS);

        assertEquals(
            parseString("{'name':{'booleans':[false],'ints':[0],'longs':[0],'doubles':[0.0],'strings':['']}}"),
            registry.represent(List.of("root", "arraysList"), jsonVisitor())
        );

        assertEquals(
            parseString("{'booleanVal':false,'intVal':0,'longVal':0,'doubleVal':0.0,'stringVal':''}"),
            registry.represent(List.of("root", "primitivesList", "name"), jsonVisitor())
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

        assertThrows(IllegalArgumentException.class, () -> registry.represent(List.of("doot"), jsonVisitor()));

        assertThrows(IllegalArgumentException.class, () -> registry.represent(List.of("root", "x"), jsonVisitor()));

        assertEquals(
            JsonNull.INSTANCE,
            registry.represent(List.of("root", "primitivesList", "foo"), jsonVisitor())
        );
    }
}
