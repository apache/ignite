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

import java.lang.reflect.Field;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * {@link FieldAccessor} implementation.
 */
class UnsafeFieldAccessor implements FieldAccessor {
    private final Field field;
    private final Class<?> fieldType;
    private final long fieldOffset;

    UnsafeFieldAccessor(String fieldName, Class<?> declaringClass) {
        this(findField(fieldName, declaringClass));
    }

    UnsafeFieldAccessor(Field field) {
        this.field = field;
        fieldType = field.getType();
        fieldOffset = GridUnsafe.objectFieldOffset(field);
    }

    private static Field findField(String fieldName, Class<?> declaringClass) {
        try {
            return declaringClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new ReflectionException("Cannot find field", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object getObject(Object target) {
        assert !fieldType.isPrimitive() : field.getDeclaringClass() + "#" + field.getName() + " is primitive!";

        return GridUnsafe.getObjectField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setObject(Object target, Object fieldValue) {
        assert !fieldType.isPrimitive() : field.getDeclaringClass() + "#" + field.getName() + " is primitive!";

        GridUnsafe.putObjectField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public byte getByte(Object target) {
        assert fieldType == byte.class : field.getDeclaringClass() + "#" + field.getName() + " is not byte";

        return GridUnsafe.getByteField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setByte(Object target, byte fieldValue) {
        assert fieldType == byte.class : field.getDeclaringClass() + "#" + field.getName() + " is not byte";

        GridUnsafe.putByteField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public short getShort(Object target) {
        assert fieldType == short.class : field.getDeclaringClass() + "#" + field.getName() + " is not short";

        return GridUnsafe.getShortField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setShort(Object target, short fieldValue) {
        assert fieldType == short.class : field.getDeclaringClass() + "#" + field.getName() + " is not short";

        GridUnsafe.putShortField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public int getInt(Object target) {
        assert fieldType == int.class : field.getDeclaringClass() + "#" + field.getName() + " is not int";

        return GridUnsafe.getIntField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setInt(Object target, int fieldValue) {
        assert fieldType == int.class : field.getDeclaringClass() + "#" + field.getName() + " is not int";

        GridUnsafe.putIntField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public long getLong(Object target) {
        assert fieldType == long.class : field.getDeclaringClass() + "#" + field.getName() + " is not long";

        return GridUnsafe.getLongField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setLong(Object target, long fieldValue) {
        assert fieldType == long.class : field.getDeclaringClass() + "#" + field.getName() + " is not long";

        GridUnsafe.putLongField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public float getFloat(Object target) {
        assert fieldType == float.class : field.getDeclaringClass() + "#" + field.getName() + " is not float";

        return GridUnsafe.getFloatField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setFloat(Object target, float fieldValue) {
        assert fieldType == float.class : field.getDeclaringClass() + "#" + field.getName() + " is not float";

        GridUnsafe.putFloatField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public double getDouble(Object target) {
        assert fieldType == double.class : field.getDeclaringClass() + "#" + field.getName() + " is not double";

        return GridUnsafe.getDoubleField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setDouble(Object target, double fieldValue) {
        assert fieldType == double.class : field.getDeclaringClass() + "#" + field.getName() + " is not double";

        GridUnsafe.putDoubleField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public char getChar(Object target) {
        assert fieldType == char.class : field.getDeclaringClass() + "#" + field.getName() + " is not char";

        return GridUnsafe.getCharField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setChar(Object target, char fieldValue) {
        assert fieldType == char.class : field.getDeclaringClass() + "#" + field.getName() + " is not char";

        GridUnsafe.putCharField(target, fieldOffset, fieldValue);
    }

    /** {@inheritDoc} */
    @Override
    public boolean getBoolean(Object target) {
        assert fieldType == boolean.class : field.getDeclaringClass() + "#" + field.getName() + " is not boolean";

        return GridUnsafe.getBooleanField(target, fieldOffset);
    }

    /** {@inheritDoc} */
    @Override
    public void setBoolean(Object target, boolean fieldValue) {
        assert fieldType == boolean.class : field.getDeclaringClass() + "#" + field.getName() + " is not boolean";

        GridUnsafe.putBooleanField(target, fieldOffset, fieldValue);
    }
}
