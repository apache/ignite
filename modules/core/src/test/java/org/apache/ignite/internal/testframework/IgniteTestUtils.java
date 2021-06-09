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

package org.apache.ignite.internal.testframework;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.ignite.lang.IgniteInternalException;

/**
 * Utility class for tests.
 */
public final class IgniteTestUtils {
    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteInternalException In case of error.
     */
    public static void setFieldValue(Object obj, String fieldName, Object val) throws IgniteInternalException {
        assert obj != null;
        assert fieldName != null;

        try {
            Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

            Field field = cls.getDeclaredField(fieldName);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /**
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic)
                throw new IgniteInternalException("Modification of static final field through reflection.");

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            field.set(obj, val);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteInternalException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param cls Class to get field from.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteInternalException In case of error.
     */
    public static void setFieldValue(Object obj, Class cls, String fieldName, Object val) throws IgniteInternalException {
        assert fieldName != null;

        try {
            Field field = cls.getDeclaredField(fieldName);

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /**
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic)
                throw new IgniteInternalException("Modification of static final field through reflection.");

            if (isFinal) {
                Field modifiersField = Field.class.getDeclaredField("modifiers");

                modifiersField.setAccessible(true);

                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            }

            field.set(obj, val);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteInternalException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }
}
