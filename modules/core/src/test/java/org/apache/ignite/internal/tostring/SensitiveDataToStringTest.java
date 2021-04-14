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

package org.apache.ignite.internal.tostring;

import java.util.Objects;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy.HASH;
import static org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy.NONE;
import static org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy.PLAIN;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for output of {@code toString()} depending on the value of
 * {@link IgniteSystemProperties#IGNITE_SENSITIVE_DATA_LOGGING}
 */
@ExtendWith(SystemPropertiesExtension.class)
public class SensitiveDataToStringTest extends IgniteAbstractTest {
    /** Random int. */
    private static final int rndInt0 = 54321;

    /** Random string. */
    private static final String rndString = "qwer";

    /**
     *
     */
    @Test
    public void testSensitivePropertiesResolving0() {
        assertSame(HASH, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testSensitivePropertiesResolving1() {
        assertSame(PLAIN, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testSensitivePropertiesResolving2() {
        assertSame(HASH, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testSensitivePropertiesResolving3() {
        assertSame(NONE, S.getSensitiveDataLogging(), S.getSensitiveDataLogging().toString());
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "plain")
    public void testTableObjectImplWithSensitive() {
        testTableObjectImpl((strToCheck, object) -> assertTrue(strToCheck.contains(object.toString()), strToCheck));
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "hash")
    public void testTableObjectImplWithHashSensitive() {
        testTableObjectImpl((strToCheck, object) -> assertTrue(strToCheck.contains(object.toString()), strToCheck));
    }

    /**
     *
     */
    @Test
    @WithSystemProperty(key = IGNITE_SENSITIVE_DATA_LOGGING, value = "none")
    public void testTableObjectImplWithoutSensitive() {
        testTableObjectImpl((strToCheck, object) -> assertEquals("TableObject", object.toString(), strToCheck));
    }

    /**
     *
     */
    private void testTableObjectImpl(BiConsumer<String, Object> checker) {
        Person person = new Person(rndInt0, rndString);

        TableObject testObject = new TableObject(person);
        checker.accept(testObject.toString(), testObject);
    }

    /**
     *
     */
    static class TableObject {
        @IgniteToStringInclude(sensitive = true)
        Person person;

        TableObject(Person person) {
            this.person = person;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            switch (S.getSensitiveDataLogging()) {
                case PLAIN:
                    return S.toString(getClass().getSimpleName(), "person", person, false);

                case HASH:
                    return String.valueOf(person == null ? "null" : IgniteUtils.hash(person));

                case NONE:
                default:
                    return "TableObject";
            }
        }
    }

    /**
     *
     */
    static class Person {
        /** Id organization. */
        int orgId;

        /** Person name. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Id organization.
         * @param name Person name.
         */
        Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(orgId, name);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
