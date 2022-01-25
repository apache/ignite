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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.ignite.internal.network.serialization.Classes;
import org.jetbrains.annotations.Nullable;

/**
 * Validations that are run before marshalling objects.
 */
class MarshallingValidations {
    static void throwIfMarshallingNotSupported(@Nullable Object object) {
        if (object == null) {
            return;
        }
        if (Enum.class.isAssignableFrom(object.getClass())) {
            return;
        }

        Class<?> objectClass = object.getClass();
        if (isInnerClass(objectClass)) {
            throw new MarshallingNotSupportedException("Non-static inner class instances are not supported for marshalling: "
                    + objectClass);
        }
        if (isCapturingClosure(objectClass)) {
            throw new MarshallingNotSupportedException("Capturing nested class instances are not supported for marshalling: " + object);
        }
        if (Classes.isLambda(objectClass) && !Classes.isSerializable(objectClass)) {
            throw new MarshallingNotSupportedException("Non-serializable lambda instances are not supported for marshalling: " + object);
        }
    }

    private static boolean isInnerClass(Class<?> objectClass) {
        return objectClass.getDeclaringClass() != null && !Modifier.isStatic(objectClass.getModifiers());
    }

    private static boolean isCapturingClosure(Class<?> objectClass) {
        for (Field field : objectClass.getDeclaredFields()) {
            if ((field.isSynthetic() && field.getName().equals("this$0"))
                    || field.getName().startsWith("arg$")) {
                return true;
            }
        }

        return false;
    }

    private MarshallingValidations() {
    }
}
