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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link SerializableInstantiation}.
 */
@ExtendWith(MockitoExtension.class)
class SerializableInstantiationTest {
    private final Instantiation instantiation = new SerializableInstantiation();

    @Test
    void doesNotSupportNonSerializableClasses() {
        assertFalse(instantiation.supports(NotSerializable.class));
    }

    @Test
    void supportsSerializableClasses() {
        assertTrue(instantiation.supports(SerializableWithoutNoArgConstructor.class));
    }

    @Test
    void supportsClassesWithWriteReplace() {
        assertTrue(instantiation.supports(WithWriteReplace.class));
    }

    @Test
    void supportsClassesWithReadResolve() {
        assertTrue(instantiation.supports(WithReadResolve.class));
    }

    @Test
    void instantiatesSerializableClassesWithoutNoArgConstructor() throws Exception {
        Object instance = instantiation.newInstance(SerializableWithoutNoArgConstructor.class);

        assertThat(instance, is(notNullValue()));
    }

    @Test
    void instantiatesSerializableClassesWithFields() throws Exception {
        Object instance = instantiation.newInstance(SerializableWithFields.class);

        assertThat(instance, is(notNullValue()));
    }

    @Test
    void instantiatesSerializableClassesWithNonSerializableParents() throws Exception {
        Object instance = instantiation.newInstance(SerializableWithNonSerializableParent.class);

        assertThat(instance, is(notNullValue()));
    }

    @Test
    void instantiatesSerializableClassesWithSerializableParents() throws Exception {
        Object instance = instantiation.newInstance(SerializableWithSerializableParent.class);

        assertThat(instance, is(notNullValue()));
    }

    private static class NotSerializable {
    }

    private static class SerializableWithoutNoArgConstructor implements Serializable {
        @SuppressWarnings("unused")
        private SerializableWithoutNoArgConstructor(int ignored) {
        }
    }

    private static class SerializableWithFields implements Serializable {
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final int value;

        private SerializableWithFields(int value) {
            this.value = value;
        }
    }

    private static class NonSerializableParent {
        public NonSerializableParent() {
        }
    }

    private static class SerializableWithNonSerializableParent extends NonSerializableParent implements Serializable {
        @SuppressWarnings("unused")
        private SerializableWithNonSerializableParent(int ignored) {
        }
    }

    private static class SerializableParent implements Serializable {
    }

    private static class SerializableWithSerializableParent extends SerializableParent implements Serializable {
        @SuppressWarnings("unused")
        private SerializableWithSerializableParent(int ignored) {
        }
    }

    private static class WithWriteReplace implements Serializable {
        private Object writeReplace() {
            return this;
        }
    }

    private static class WithReadResolve implements Serializable {
        private Object readResolve() {
            return this;
        }
    }
}
