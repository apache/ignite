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

package org.apache.ignite.internal.util.tostring;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.ignite.internal.util.GridUnsafe;
import org.intellij.lang.annotations.MagicConstant;

/**
 * Simple field descriptor containing field name and its order in the class descriptor.
 */
class GridToStringFieldDescriptor {
    /** */
    public static final int FIELD_TYPE_OBJECT = 0;

    /** */
    public static final int FIELD_TYPE_BYTE = 1;

    /** */
    public static final int FIELD_TYPE_BOOLEAN = 2;

    /** */
    public static final int FIELD_TYPE_CHAR = 3;

    /** */
    public static final int FIELD_TYPE_SHORT = 4;

    /** */
    public static final int FIELD_TYPE_INT = 5;

    /** */
    public static final int FIELD_TYPE_FLOAT = 6;

    /** */
    public static final int FIELD_TYPE_LONG = 7;

    /** */
    public static final int FIELD_TYPE_DOUBLE = 8;

    /** Field name. */
    private final String name;

    /** */
    private int order = Integer.MAX_VALUE;

    /** Field offset as returned by {@link GridUnsafe#objectFieldOffset(java.lang.reflect.Field)}. */
    private final long off;

    /** Numeric constant for the field's type. One of {@code FIELD_TYPE_*} constants of current class. */
    private final int type;

    /** Class of the field. Upper bound in case of generic field types. */
    private final Class<?> cls;

    /** */
    GridToStringFieldDescriptor(Field field) {
        assert (field.getModifiers() & Modifier.STATIC) == 0 : "Static fields are not allowed here: " + field;

        off = GridUnsafe.objectFieldOffset(field);

        cls = field.getType();

        name = field.getName();

        if (!cls.isPrimitive())
            type = FIELD_TYPE_OBJECT;
        else {
            if (cls == byte.class)
                type = FIELD_TYPE_BYTE;
            else if (cls == boolean.class)
                type = FIELD_TYPE_BOOLEAN;
            else if (cls == char.class)
                type = FIELD_TYPE_CHAR;
            else if (cls == short.class)
                type = FIELD_TYPE_SHORT;
            else if (cls == int.class)
                type = FIELD_TYPE_INT;
            else if (cls == float.class)
                type = FIELD_TYPE_FLOAT;
            else if (cls == long.class)
                type = FIELD_TYPE_LONG;
            else if (cls == double.class)
                type = FIELD_TYPE_DOUBLE;
            else
                throw new IllegalArgumentException("Unexpected primitive type: " + cls);
        }
    }

    /**
     * @return Field order.
     */
    int getOrder() { return order; }

    /**
     * @param order Field order.
     */
    void setOrder(int order) { this.order = order; }

    /**
     * @return Field offset as returned by {@link GridUnsafe#objectFieldOffset(java.lang.reflect.Field)}.
     */
    public long offset() {
        return off;
    }

    /**
     * @return Numeric constant for the field's type. One of {@code FIELD_TYPE_*} constants of current class.
     */
    @MagicConstant(valuesFromClass = GridToStringFieldDescriptor.class)
    public int type() {
        return type;
    }

    /** */
    public Class<?> fieldClass() {
        return cls;
    }

    /**
     * @return Field name.
     */
    String getName() { return name; }
}
