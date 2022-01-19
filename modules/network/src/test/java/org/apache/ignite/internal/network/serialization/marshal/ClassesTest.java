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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import org.junit.jupiter.api.Test;

class ClassesTest {
    @Test
    void isSerializableReturnsFalseForNonSerializableClass() {
        assertFalse(Classes.isSerializable(NonSerializable.class));
    }

    @Test
    void isSerializableReturnsTrueForSerializableClass() {
        assertTrue(Classes.isSerializable(EmptySerializable.class));
    }

    @Test
    void isLambdaReturnsFalseForOrdinaryClassInstance() {
        assertFalse(Classes.isLambda(NonSerializable.class));
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

    private static class NonSerializable {
    }

    private static class EmptySerializable implements Serializable {
    }

    private enum EmptyEnum {
    }
}
