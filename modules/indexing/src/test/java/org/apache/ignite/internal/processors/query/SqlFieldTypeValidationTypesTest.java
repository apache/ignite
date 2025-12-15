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
package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 *
 */
@ParameterizedClass(name = "{index} type={0}, errVal={1}, indexed={2}")
@MethodSource("allTypesArgs")
public class SqlFieldTypeValidationTypesTest extends AbstractIndexingCommonTest {
    /** */
    private static final String ERROR = "Type for a column 'NAME' is not compatible with table definition.";

    /** */
    @Parameter(0)
    public Class<?> fieldType;

    /** */
    @Parameter(1)
    public Object errVal;

    /** */
    @Parameter(2)
    public boolean indexed;

    /** */
    private static boolean validate;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setValidationEnabled(validate);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setQueryEntities(
                Collections.singletonList(
                        new QueryEntity()
                                .setKeyFieldName("id")
                                .setValueType("Person")
                                .setFields(new LinkedHashMap<>(
                                        F.asMap("id", "java.lang.Integer",
                                                "name", fieldType.getName())))
                                .setIndexes(indexed ? Collections.singletonList(new QueryIndex("name")) : null)
        )));

        cfg.setBinaryConfiguration(new BinaryConfiguration().setNameMapper(new BinaryBasicNameMapper(true)));

        return cfg;
    }

    /** */
    private static Stream<Arguments> allTypesArgs() {
        List<Arguments> params = new ArrayList<>();

        // Type, ok value, error value, indexed flag.
        params.add(Arguments.of(Integer.class, "1", false));
        params.add(Arguments.of(Integer.class, "1", true));

        params.add(Arguments.of(String.class, 1, false));
        params.add(Arguments.of(String.class, 1, true));

        params.add(Arguments.of(UUID.class, UUID.randomUUID().toString(), false));
        params.add(Arguments.of(UUID.class, UUID.randomUUID().toString(), true));

        params.add(Arguments.of(TestEnum.class, "C", false));
        params.add(Arguments.of(TestEnum.class, "C", true));

        params.add(Arguments.of(Integer[].class, new String[] {"0", "1"}, false));
        params.add(Arguments.of(Integer[].class, new String[] {"0", "1"}, true));

        params.add(Arguments.of(int[].class, new String[] {"0", "1"}, false));
        params.add(Arguments.of(int[].class, new String[] {"0", "1"}, true));

        params.add(Arguments.of(int[].class, new Object[] {"0", "1"}, false));
        params.add(Arguments.of(int[].class, new Object[] {"0", "1"}, true));

        return params.stream();
    }

    /** */
    @AfterEach
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testError() throws Exception {
        validate = true;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        BinaryObject obj = grid(0).binary().builder("Person")
                .setField("id", 1)
                .setField("name", errVal)
                .build();

        GridTestUtils.assertThrows(log, () -> cache.put(1, obj), IgniteException.class, ERROR);

        assertNull(cache.withKeepBinary().get(1));
    }

    /** */
    @Test
    public void testClassInstanceError() throws Exception {
        validate = true;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        GridTestUtils.assertThrows(log, () -> cache.put(1, new Person(errVal)), IgniteException.class, ERROR);

        assertNull(cache.withKeepBinary().get(1));
    }

    /** */
    public static class Person {
        /** */
        private final Object name;

        /** */
        public Person(Object name) {
            this.name = name;
        }

        /** */
        public Object name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof Person))
                return false;

            if (name instanceof Object[])
                return Arrays.equals((Object[])name, (Object[])((Person)obj).name);

            return Objects.equals(name, ((Person)obj).name);
        }
    }

    /**
     *
     */
    public enum TestEnum {
        /** */
        A,

        /** */
        B
    }
}
