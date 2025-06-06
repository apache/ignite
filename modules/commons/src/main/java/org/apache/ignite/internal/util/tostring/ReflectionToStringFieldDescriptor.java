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
import org.apache.ignite.IgniteException;

/**
 * Simple field descriptor containing field name and its order in the class descriptor.
 */
class ReflectionToStringFieldDescriptor extends GridToStringFieldDescriptor {
    /** Field. */
    private final Field field;

    /**
     * @param field Field;
     */
    ReflectionToStringFieldDescriptor(Field field) {
        super(field);

        this.field = field;

        field.setAccessible(true);
    }

    /** {@inheritDoc} */
    @Override long offset(Field field) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Object objectValue(Object obj) {
        try {
            return field.get(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(Object obj) {
        try {
            return field.getByte(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean booleanValue(Object obj) {
        try {
            return field.getBoolean(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public char charValue(Object obj) {
        try {
            return field.getChar(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short shortValue(Object obj) {
        try {
            return field.getShort(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int intField(Object obj) {
        try {
            return field.getInt(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public float floatField(Object obj) {
        try {
            return field.getFloat(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long longField(Object obj) {
        try {
            return field.getLong(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public double doubleField(Object obj) {
        try {
            return field.getDouble(obj);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }
}
