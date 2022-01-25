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

package org.apache.ignite.internal.network.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClassesTest {
    @Test
    void isSerializableReturnsFalseForNonSerializableClass() {
        assertFalse(Classes.isSerializable(Object.class));
    }

    @Test
    void isSerializableReturnsTrueForSerializableClass() {
        assertTrue(Classes.isSerializable(EmptySerializable.class));
    }

    @Test
    void isLambdaReturnsFalseForOrdinaryClassInstance() {
        assertFalse(Classes.isLambda(Object.class));
    }

    @SuppressWarnings("Convert2Lambda")
    @Test
    void isLambdaReturnsFalseForAnonymousClassInstance() {
        Runnable object = new Runnable() {
            @Override
            public void run() {
                // no-op
            }
        };

        assertFalse(Classes.isLambda(object.getClass()));
    }

    @Test
    void isLambdaReturnsTrueForNonSerializableLambda() {
        Runnable object = () -> {};

        assertTrue(Classes.isLambda(object.getClass()));
    }

    @Test
    void isLambdaReturnsTrueForSerializableLambda() {
        Runnable object = serializableLambda();

        assertTrue(Classes.isLambda(object.getClass()));
    }

    private Runnable serializableLambda() {
        return (Runnable & Serializable) () -> {};
    }

    @Test
    void isLambdaReturnsFalseForPrimitiveClasses() {
        assertFalse(Classes.isLambda(int.class));
    }

    @Test
    void isLambdaReturnsFalseForPrimitiveArrayClasses() {
        assertFalse(Classes.isLambda(int[].class));
    }

    @Test
    void isLambdaReturnsFalseForObjectArrayClasses() {
        assertFalse(Classes.isLambda(Object[].class));
    }

    @Test
    void isLambdaReturnsFalseForEnumClasses() {
        assertFalse(Classes.isLambda(EmptyEnum.class));
    }

    @Test
    void isExternalizableReturnsFalseForNonExternalizable() {
        assertFalse(Classes.isExternalizable(Object.class));
    }

    @Test
    void isExternalizableReturnsTrueForExternalizableClass() {
        assertTrue(Classes.isExternalizable(EmptyExternalizable.class));
    }

    @Test
    void isRuntimeEnumReturnsFalseForNonEnum() {
        assertFalse(Classes.isRuntimeEnum(Object.class));
    }

    @Test
    void isRuntimeEnumReturnsTrueForSimpleEnums() {
        assertTrue(Classes.isRuntimeEnum(SimpleEnum.class));
    }

    @Test
    void isRuntimeEnumReturnsTrueForAnonClassesForMembersOfEnum() {
        assertTrue(Classes.isRuntimeEnum(EnumWithAnonClassMember.class));
    }

    @Test
    void isRuntimeEnumReturnsFalseForAbstractEnumClass() {
        assertFalse(Classes.isRuntimeEnum(Enum.class));
    }

    @Test
    void enumClassAsInSourceCodeReturnsTheClassItselfForSimpleEnums() {
        assertThat(Classes.enumClassAsInSourceCode(SimpleEnum.MEMBER.getClass()), is(SimpleEnum.class));
    }

    @Test
    void enumClassAsInSourceCodeReturnsTheClassItselfForAnonymousEnumSubclasses() {
        assertThat(Classes.enumClassAsInSourceCode(EnumWithAnonClassMember.MEMBER.getClass()), is(EnumWithAnonClassMember.class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsTrueForFinalNonArrayClasses() {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(FinalClass.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {byte.class, short.class, int.class, long.class, float.class, double.class, char.class, boolean.class})
    void isValueTypeKnownUpfrontReturnsTrueForPrimitiveClasses(Class<?> primitiveClass) {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(primitiveClass));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsFalseForNonFinalNonArrayClasses() {
        assertFalse(Classes.isRuntimeTypeKnownUpfront(NonFinalClass.class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsTrueForSimpleEnumClasses() {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(SimpleEnum.class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsTrueForEnumClassesWithAnonMembers() {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(EnumWithAnonClassMember.class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsFalseForAbstractEnum() {
        assertFalse(Classes.isRuntimeTypeKnownUpfront(Enum.class));
    }

    @ParameterizedTest
    @ValueSource(classes = {
            byte[].class, short[].class, int[].class, long[].class,
            float[].class, double[].class, char[].class, boolean[].class
    })
    void isValueTypeKnownUpfrontReturnsTrueForPrimitiveArrayClasses(Class<?> primitiveArrayClass) {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(primitiveArrayClass));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsTrueForArraysOfFinalClasses() {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(FinalClass[].class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsFalseForArraysOfNonFinalClasses() {
        assertFalse(Classes.isRuntimeTypeKnownUpfront(NonFinalClass[].class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsTrueForArraysOfSimpleEnumClasses() {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(SimpleEnum[].class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsTrueForArraysOfEnumClassesWithAnonMembers() {
        assertTrue(Classes.isRuntimeTypeKnownUpfront(EnumWithAnonClassMember[].class));
    }

    @Test
    void isValueTypeKnownUpfrontReturnsFalseForArraysOfAbstractEnum() {
        assertFalse(Classes.isRuntimeTypeKnownUpfront(Enum[].class));
    }

    private static class EmptySerializable implements Serializable {
    }

    private enum EmptyEnum {
    }

    private static class EmptyExternalizable implements Externalizable {
        public EmptyExternalizable() {
        }

        @Override
        public void writeExternal(ObjectOutput out) {
            // no-op
        }

        @Override
        public void readExternal(ObjectInput in) {
            // no-op
        }
    }

    private enum SimpleEnum {
        MEMBER
    }

    private enum EnumWithAnonClassMember {
        MEMBER {
        }
    }

    private static final class FinalClass {
    }

    private static class NonFinalClass {
    }
}
